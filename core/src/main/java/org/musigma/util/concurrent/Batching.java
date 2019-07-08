package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.function.UncheckedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

// based on:
// https://github.com/scala/scala/pull/7663
class Batching {

    private static final int SYNC_PRE_BATCH_DEPTH = 16;
    private static final int RUN_LIMIT = 1024;

    private static String getString(final String propertyName, final String defaultValue) {
        try {
            return System.getProperty(propertyName, defaultValue);
        } catch (final SecurityException ignored) {
            return defaultValue;
        }
    }

    private static int getInt(final String propertyName, final String defaultValue) {
        final String s = getString(propertyName, defaultValue);
        if (s.charAt(0) == 'x') {
            return (int) Math.ceil(Double.parseDouble(s.substring(1)) * Runtime.getRuntime().availableProcessors());
        } else {
            return Integer.parseInt(s);
        }
    }

    static DefaultExecutorService createDefaultExecutor(final Consumer<Throwable> reporter) {
        final int parallelism = Math.min(
                Math.max(getInt("org.musigma.util.concurrent.minThreads", "1"),
                        getInt("org.musigma.util.concurrent.numThreads", "x1")),
                getInt("org.musigma.util.concurrent.maxThreads", "x1"));
        final int maxBlockers = getInt("org.musigma.util.concurrent.maxExtraThreads", "256");
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> reporter.accept(e);
        final DefaultThreadFactory threadFactory = new DefaultThreadFactory(true, maxBlockers, "global-scheduler", uncaughtExceptionHandler);
        return new DefaultExecutorService(parallelism, threadFactory, uncaughtExceptionHandler);
    }

    interface BatchingExecutorService<Task extends AbstractTask> extends ErrorReportingExecutorService {
        Task getCurrentTask();

        void setCurrentTask(final Task task);

        void submitForExecution(final Runnable runnable);

        void batch(final Runnable runnable);
    }

    abstract static class AbstractTask<BatchingES extends BatchingExecutorService> {
        final BatchingES executorService;
        // TODO: this can be optimized by unboxing the first Runnable
        final List<Runnable> runnables;

        AbstractTask(final BatchingES executorService, final Runnable runnable) {
            this(executorService, Collections.singletonList(runnable));
        }

        AbstractTask(final BatchingES executorService, final List<Runnable> runnables) {
            this.executorService = executorService;
            this.runnables = new ArrayList<>(runnables);
        }

        boolean isBlocking() {
            return !runnables.isEmpty();
        }

        void push(final Runnable runnable) {
            runnables.add(runnable);
        }

        void runN(final int n) {
            if (n < 0) {
                throw new IllegalArgumentException("n must be non-negative");
            }
            for (int i = 0; i < n && i < runnables.size(); i++) {
                runnables.remove(0).run();
            }
        }
    }

    interface AsynchronousBatchingExecutorService extends BatchingExecutorService<AsynchronousTask> {
        ThreadLocal<AsynchronousTask> asyncContext();

        @Override
        default AsynchronousTask getCurrentTask() {
            return asyncContext().get();
        }

        @Override
        default void setCurrentTask(final AsynchronousTask task) {
            asyncContext().set(task);
        }

        default void clearCurrentTask() {
            asyncContext().remove();
        }

        @Override
        default void batch(final Runnable runnable) {
            Objects.requireNonNull(runnable);
            AsynchronousTask task = getCurrentTask();
            if (task != null) {
                task.push(runnable);
            } else {
                submitForExecution(new AsynchronousTask(this, runnable));
            }
        }
    }

    static class AsynchronousTask extends AbstractTask<AsynchronousBatchingExecutorService> implements Runnable, BlockContext, UncheckedFunction<BlockContext, Throwable> {

        private static final BlockContext MISSING_PARENT_BLOCK_CONTEXT = new BlockContext() {
            @Override
            public <T> T blockOn(final Callable<T> thunk) throws Exception {
                throw new IllegalStateException("missing parent block context");
            }
        };

        private BlockContext parentBlockContext = MISSING_PARENT_BLOCK_CONTEXT;

        AsynchronousTask(final AsynchronousBatchingExecutorService executorService, final Runnable runnable) {
            super(executorService, runnable);
        }

        AsynchronousTask(final AsynchronousBatchingExecutorService executorService, final List<Runnable> runnables) {
            super(executorService, runnables);
        }

        @Override
        public void run() {
            executorService.setCurrentTask(this); // later cleared in apply()
            Throwable failure;
            try {
                failure = resubmit(this.using(this));
            } catch (final Throwable throwable) {
                failure = throwable;
            }
            if (failure != null) {
                Exceptions.rethrowUnchecked(failure);
            }
        }

        @Override
        public <T> T blockOn(final Callable<T> thunk) throws Exception {
            if (isBlocking()) {
                executorService.submitForExecution(cloneAndClear());
            }
            return parentBlockContext.blockOn(thunk);
        }

        @Override
        public Throwable apply(final BlockContext value) throws Exception {
            try {
                parentBlockContext = value;
                runN(RUN_LIMIT);
                return null;
            } catch (final Throwable throwable) {
                return throwable;
            } finally {
                parentBlockContext = MISSING_PARENT_BLOCK_CONTEXT;
                executorService.clearCurrentTask();
            }
        }

        private Throwable resubmit(final Throwable throwable) {
            if (!isBlocking()) {
                return throwable;
            }
            try {
                executorService.submitForExecution(this);
                return throwable;
            } catch (final Throwable t) {
                if (Exceptions.isFatal(t)) {
                    return t;
                }
                ExecutionException e = new ExecutionException("non-fatal error occurred and resubmission failed; check suppressed exception", throwable);
                e.addSuppressed(t);
                return e;
            }
        }

        private AsynchronousTask cloneAndClear() {
            AsynchronousTask task = new AsynchronousTask(executorService, runnables);
            runnables.clear();
            return task;
        }
    }

    interface SynchronousBatchingExecutorService extends BatchingExecutorService<SynchronousTask> {
        int getPreBatchTaskCount();

        void setPreBatchTaskCount(final int count);

        @Override
        default void batch(final Runnable runnable) {
            Objects.requireNonNull(runnable);
            SynchronousTask currentTask = getCurrentTask();
            if (currentTask != null) {
                currentTask.push(runnable);
            } else {
                int i = getPreBatchTaskCount();
                try {
                    if (i < SYNC_PRE_BATCH_DEPTH) {
                        setPreBatchTaskCount(i + 1);
                        submitForExecution(runnable);
                    } else {
                        SynchronousTask task = new SynchronousTask(this, runnable);
                        setCurrentTask(task);
                        submitForExecution(runnable);
                    }
                } catch (final Throwable t) {
                    Exceptions.rethrowIfFatal(t);
                    reportFailure(t);
                } finally {
                    setPreBatchTaskCount(i);
                }
            }
        }
    }

    static class SynchronousTask extends AbstractTask<SynchronousBatchingExecutorService> implements Runnable {
        SynchronousTask(final SynchronousBatchingExecutorService executorService, final Runnable runnable) {
            super(executorService, runnable);
        }

        @Override
        public void run() {
            while (isBlocking()) {
                try {
                    runN(RUN_LIMIT);
                } catch (final Throwable throwable) {
                    Exceptions.rethrowIfFatal(throwable);
                    executorService.reportFailure(throwable);
                }
            }
        }
    }

    static class DefaultThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory {
        private final boolean daemonic;
        private final String prefix;
        private final Thread.UncaughtExceptionHandler uncaught;
        private final Semaphore blockerPermits;

        DefaultThreadFactory(final boolean daemonic, final int maxBlockers, final String prefix, final Thread.UncaughtExceptionHandler exceptionHandler) {
            this.daemonic = daemonic;
            if (maxBlockers < 0) throw new IllegalArgumentException("maxBlockers must be non-negative");
            this.prefix = Objects.requireNonNull(prefix);
            this.uncaught = exceptionHandler;
            blockerPermits = new Semaphore(maxBlockers);
        }

        private <T extends Thread> T wire(final T thread) {
            thread.setDaemon(daemonic);
            thread.setUncaughtExceptionHandler(uncaught);
            thread.setName(prefix + '-' + thread.getId());
            return thread;
        }

        @Override
        public Thread newThread(final Runnable r) {
            return wire(new Thread(r));
        }

        @Override
        public ForkJoinWorkerThread newThread(final ForkJoinPool pool) {
            return wire(new DefaultForkJoinWorkerThread(pool));
        }

        private class DefaultForkJoinWorkerThread extends ForkJoinWorkerThread implements BlockContext {
            private boolean blocked;

            private DefaultForkJoinWorkerThread(final ForkJoinPool pool) {
                super(pool);
            }

            @Override
            public <T> T blockOn(final Callable<T> thunk) throws Exception {
                if (Thread.currentThread() == this && !blocked && blockerPermits.tryAcquire()) {
                    try {
                        final ManagedBlockerThunk<T> blocker = new ManagedBlockerThunk<>(thunk);
                        blocked = true;
                        ForkJoinPool.managedBlock(blocker);
                        return blocker.result;
                    } finally {
                        blocked = false;
                        blockerPermits.release();
                    }
                } else {
                    // unmanaged blocking
                    return thunk.call();
                }
            }

            private class ManagedBlockerThunk<T> implements ForkJoinPool.ManagedBlocker {
                private final Callable<T> thunk;
                private T result;
                private boolean done;

                private ManagedBlockerThunk(final Callable<T> thunk) {
                    this.thunk = thunk;
                }

                @Override
                public boolean block() throws InterruptedException {
                    if (!done) {
                        try {
                            result = thunk.call();
                            done = true;
                        } catch (final Throwable throwable) {
                            Exceptions.rethrowUnchecked(throwable);
                            return false;
                        }
                    }
                    return isReleasable();
                }

                @Override
                public boolean isReleasable() {
                    return done;
                }
            }
        }
    }

    static class DefaultExecutorService extends ForkJoinPool implements AsynchronousBatchingExecutorService {
        private final ThreadLocal<AsynchronousTask> asyncContext = new ThreadLocal<>();

        private DefaultExecutorService(final int parallelism, final ForkJoinWorkerThreadFactory factory, final Thread.UncaughtExceptionHandler handler) {
            super(parallelism, factory, handler, true);
        }

        @Override
        public ThreadLocal<AsynchronousTask> asyncContext() {
            return asyncContext;
        }

        @Override
        public void submitForExecution(final Runnable runnable) {
            super.execute(runnable);
        }

        @Override
        public void execute(final Runnable task) {
            if ((!(task instanceof Transformation) || ((Transformation<?, ?>) task).benefitsFromBatching())
                    && task.getClass().isAnnotationPresent(Batchable.class)) {
                batch(task);
            } else {
                submitForExecution(task);
            }
        }

        @Override
        public void reportFailure(final Throwable error) {
            final Thread.UncaughtExceptionHandler handler = getUncaughtExceptionHandler();
            if (handler != null) {
                handler.uncaughtException(Thread.currentThread(), error);
            }
        }
    }

    static class ParasiticExecutorService extends AbstractExecutorService implements SynchronousBatchingExecutorService {

        private final ThreadLocal<Object> syncContext = new ThreadLocal<>();

        @Override
        public int getPreBatchTaskCount() {
            Object o = syncContext.get();
            return o instanceof Integer ? (Integer) o : 0;
        }

        @Override
        public void setPreBatchTaskCount(final int count) {
            if (count < 0) {
                throw new IllegalArgumentException("count must be non-negative");
            }
            if (count == 0) {
                syncContext.remove();
            } else {
                syncContext.set(count);
            }
        }

        @Override
        public SynchronousTask getCurrentTask() {
            Object o = syncContext.get();
            return o instanceof SynchronousTask ? (SynchronousTask) o : null;
        }

        @Override
        public void setCurrentTask(final SynchronousTask task) {
            syncContext.set(task);
        }

        @Override
        public void submitForExecution(final Runnable runnable) {
            runnable.run();
        }

        @Override
        public void reportFailure(final Throwable error) {
            error.printStackTrace();
        }

        @Override
        public void execute(final Runnable command) {
            batch(command);
        }

        @Override
        public void shutdown() {
            // no-op
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return false;
        }
    }
}
