package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class Blocking {

    public static <T> T blocking(final Callable<T> callable) throws Exception {
        return Blocker.current().blockOn(callable);
    }

    static BatchingExecutor newGlobalExecutor(final Thread.UncaughtExceptionHandler uncaught) {
        int parallelism = Math.min(
                Math.max(getInt("org.musigma.util.concurrent.minThreads", "1"),
                        getInt("org.musigma.util.concurrent.numThreads", "x1")),
                getInt("org.musigma.util.concurrent.maxThreads", "x1"));
        int maxBlockers = getInt("org.musigma.util.concurrent.maxExtraThreads", "256");
        BlockingThreadFactory factory = new BlockingThreadFactory(true, maxBlockers, "global-executor", uncaught);
        ForkJoinPool pool = new ForkJoinPool(parallelism, factory, uncaught, true);
        return new AsynchronousBatchingExecutor(pool, t -> uncaught.uncaughtException(Thread.currentThread(), t));
    }

    static Executor newParasiticExecutor(final Thread.UncaughtExceptionHandler uncaught) {
        return new SynchronousBatchingExecutor(Runnable::run, t -> uncaught.uncaughtException(Thread.currentThread(), t));
    }

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

    interface Blocker {
        <T> T blockOn(final Callable<T> callable) throws Exception;

        default <T> T using(final UncheckedFunction<? super Blocker, ? extends T> function) throws Exception {
            Blocker previous = CURRENT.get();
            if (previous == this) {
                return function.apply(prefer(this));
            }
            CURRENT.set(this);
            try {
                return function.apply(prefer(previous));
            } finally {
                CURRENT.set(previous);
            }
        }

        static Blocker prefer(final Blocker previous) {
            if (previous != null) {
                return previous;
            }
            Thread current = Thread.currentThread();
            return current instanceof Blocker ? (Blocker) current : DEFAULT;
        }

        static Blocker current() {
            return prefer(CURRENT.get());
        }
    }

    private static final ThreadLocal<Blocker> CURRENT = new ThreadLocal<>();
    private static final Blocker DEFAULT = Callable::call;
    private static final Blocker MISSING_PARENT_BLOCKER = new Blocker() {
        @Override
        public <T> T blockOn(final Callable<T> callable) throws Exception {
            throw new IllegalStateException("missing parent blocker");
        }
    };

    interface Batcher {
        void batch(final Runnable command);
    }

    static abstract class BatchingExecutor implements Executor, Batcher, AutoCloseable {
        // this number might be tunable based on ForkJoinPool
        private static final int RUN_LIMIT = 1024;

        abstract class Batch implements ForkJoinPool.ManagedBlocker, Blocker, Batcher, Runnable {
            // TODO: this can be optimized by unboxing the first Runnable
            final List<Runnable> tasks;

            Blocker parentBlocker = MISSING_PARENT_BLOCKER;

            Batch(final Runnable task) {
                this(Collections.singletonList(task));
            }

            Batch(final List<Runnable> tasks) {
                this.tasks = new ArrayList<>(tasks);
            }

            // may execute all the tasks in this batch up to RUN_LIMIT
            @Override
            public boolean block() throws InterruptedException {
                if (tasks.isEmpty()) {
                    return true;
                }
                int limit = Math.min(RUN_LIMIT, tasks.size());
                for (int i = 0; i < limit; i++) {
                    tasks.remove(i).run();
                }
                return tasks.isEmpty();
            }

            @Override
            public boolean isReleasable() {
                return tasks.isEmpty();
            }

            @Override
            public void batch(final Runnable command) {
                tasks.add(command);
            }

            @Override
            public <T> T blockOn(final Callable<T> callable) throws Exception {
                if (!isReleasable()) {
                    executor.execute(cloneAndClear());
                }
                return parentBlocker.blockOn(callable);
            }

            abstract Batch cloneAndClear();
        }

        final Executor executor;
        final Consumer<Throwable> exceptionHandler;

        BatchingExecutor(final Executor executor, final Consumer<Throwable> exceptionHandler) {
            this.executor = Objects.requireNonNull(executor);
            this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
        }

        abstract boolean benefitsFromBatching(final Runnable command);

        @Override
        public void execute(final Runnable command) {
            if (benefitsFromBatching(command)) {
                batch(command);
            } else {
                executor.execute(command);
            }
        }

        @Override
        public void close() {
            if (executor instanceof ExecutorService) {
                ((ExecutorService) executor).shutdown();
            }
        }
    }

    static class SynchronousBatchingExecutor extends BatchingExecutor {
        private static final int SYNC_PRE_BATCH_DEPTH = 16;

        class SyncBatch extends Batch {

            SyncBatch(final Runnable task) {
                super(task);
            }

            SyncBatch(final List<Runnable> tasks) {
                super(tasks);
            }

            @Override
            public void run() {
                try {
                    ForkJoinPool.managedBlock(this);
                } catch (final InterruptedException e) {
                    exceptionHandler.accept(e);
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    exceptionHandler.accept(e);
                }
            }

            @Override
            Batch cloneAndClear() {
                SyncBatch batch = new SyncBatch(tasks);
                tasks.clear();
                return batch;
            }
        }

        private final ThreadLocal<Object> syncContext = new ThreadLocal<>();

        SynchronousBatchingExecutor(final Executor executor, final Consumer<Throwable> exceptionHandler) {
            super(executor, exceptionHandler);
        }

        @Override
        public void batch(final Runnable command) {
            Objects.requireNonNull(command);
            Object o = syncContext.get();
            if (o instanceof SyncBatch) {
                ((SyncBatch) o).batch(command);
            } else {
                int i = o instanceof Integer ? (Integer) o : 0;
                try {
                    if (i < SYNC_PRE_BATCH_DEPTH) {
                        syncContext.set(i + 1);
                        executor.execute(command);
                    } else {
                        SyncBatch batch = new SyncBatch(command);
                        syncContext.set(batch);
                        executor.execute(batch);
                    }
                } catch (final Exception e) {
                    exceptionHandler.accept(e);
                } finally {
                    syncContext.set(o);
                }
            }
        }

        @Override
        boolean benefitsFromBatching(final Runnable command) {
            return true;
        }
    }

    static class AsynchronousBatchingExecutor extends BatchingExecutor {

        class AsyncBatch extends Batch {

            AsyncBatch(final Runnable task) {
                super(task);
            }

            AsyncBatch(final List<Runnable> tasks) {
                super(tasks);
            }

            @Override
            public void run() {
                asyncContext.set(this);
                Exception error;
                try {
                    error = resubmit(using(blocker -> {
                        try {
                            parentBlocker = blocker;
                            ForkJoinPool.managedBlock(this);
                            return null;
                        } catch (final Exception e) {
                            return e;
                        } finally {
                            parentBlocker = MISSING_PARENT_BLOCKER;
                            asyncContext.remove();
                        }
                    }));
                } catch (final InterruptedException e) {
                    exceptionHandler.accept(e);
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    error = e;
                }
                if (error != null) {
                    exceptionHandler.accept(error);
                }
            }

            private Exception resubmit(final Exception error) {
                if (isReleasable()) {
                    return error;
                }
                try {
                    executor.execute(this);
                    return error;
                } catch (final Exception e) {
                    ExecutionException ee = new ExecutionException(
                            "non-fatal error occurred and resubmission failed; check suppressed exception", error);
                    ee.addSuppressed(e);
                    return ee;
                }
            }

            @Override
            Batch cloneAndClear() {
                AsyncBatch batch = new AsyncBatch(tasks);
                tasks.clear();
                return batch;
            }
        }

        private final ThreadLocal<AsyncBatch> asyncContext = new ThreadLocal<>();

        AsynchronousBatchingExecutor(final Executor executor, final Consumer<Throwable> exceptionHandler) {
            super(executor, exceptionHandler);
        }

        @Override
        public void batch(final Runnable command) {
            Objects.requireNonNull(command);
            AsyncBatch batch = asyncContext.get();
            if (batch != null) {
                batch.batch(command);
            } else {
                executor.execute(new AsyncBatch(command));
            }
        }

        @Override
        boolean benefitsFromBatching(final Runnable command) {
            return command.getClass().isAnnotationPresent(Batchable.class) &&
                    (!(command instanceof Transformation) || ((Transformation<?, ?>) command).benefitsFromBatching());
        }
    }

    static class ManagedThunk<T> implements ForkJoinPool.ManagedBlocker, Callable<T> {
        private final Callable<T> thunk;
        private Thunk<T> result;
        private boolean done;

        ManagedThunk(final Callable<T> thunk) {
            this.thunk = thunk;
        }

        @Override
        public boolean block() throws InterruptedException {
            if (!done) {
                result = Thunk.from(thunk);
                done = true;
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            return done;
        }

        @Override
        public T call() throws Exception {
            ForkJoinPool.managedBlock(this);
            return result.call();
        }
    }

    static class BlockingThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory {

        private class BlockingThread extends ForkJoinWorkerThread implements Blocker {
            private boolean blocked;

            private BlockingThread(final ForkJoinPool pool) {
                super(pool);
            }

            @Override
            public <T> T blockOn(final Callable<T> callable) throws Exception {
                if (Thread.currentThread() == this && !blocked && blockerPermits.tryAcquire()) {
                    blocked = true;
                    try {
                        return new ManagedThunk<>(callable).call();
                    } finally {
                        blocked = false;
                        blockerPermits.release();
                    }
                } else {
                    // unmanaged blocking
                    return callable.call();
                }
            }
        }

        private final boolean daemonic;
        private final Semaphore blockerPermits;
        private final String prefix;
        private final Thread.UncaughtExceptionHandler uncaught;

        BlockingThreadFactory(final boolean daemonic, final int maxBlockers, final String prefix, final Thread.UncaughtExceptionHandler uncaught) {
            this.daemonic = daemonic;
            if (maxBlockers < 0) throw new IllegalArgumentException("maxBlockers must be non-negative");
            this.blockerPermits = new Semaphore(maxBlockers);
            this.prefix = Objects.requireNonNull(prefix);
            this.uncaught = uncaught;
        }

        private <T extends Thread> T wire(final T thread) {
            thread.setDaemon(daemonic);
            thread.setUncaughtExceptionHandler(uncaught);
            thread.setName(prefix + '-' + thread.getId());
            return thread;
        }

        @Override
        public ForkJoinWorkerThread newThread(final ForkJoinPool pool) {
            return wire(new BlockingThread(pool));
        }

        @Override
        public Thread newThread(final Runnable r) {
            return wire(new Thread(r));
        }
    }

}
