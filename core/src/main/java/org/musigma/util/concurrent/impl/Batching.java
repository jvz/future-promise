package org.musigma.util.concurrent.impl;

import org.apiguardian.api.API;
import org.musigma.util.concurrent.Batchable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

@API(status = API.Status.INTERNAL)
public final class Batching {

    private Batching() {
    }

    private static final Blocking.Blocker MISSING_PARENT_BLOCKER = new Blocking.Blocker() {
        @Override
        public <T> T blockOn(final Callable<T> callable) throws Exception {
            throw new IllegalStateException("missing parent blocker");
        }
    };

    public static BatchingExecutor newBatchingExecutor(final Thread.UncaughtExceptionHandler uncaught) {
        int parallelism = Math.min(
                Math.max(getInt("org.musigma.util.concurrent.minThreads", "1"),
                        getInt("org.musigma.util.concurrent.numThreads", "x1")),
                getInt("org.musigma.util.concurrent.maxThreads", "x1"));
        int maxBlockers = getInt("org.musigma.util.concurrent.maxExtraThreads", "256");
        Blocking.BlockingThreadFactory factory = new Blocking.BlockingThreadFactory(true, maxBlockers, "global-executor", uncaught);
        ForkJoinPool pool = new ForkJoinPool(parallelism, factory, uncaught, true);
        return new AsynchronousBatchingExecutor(pool, t -> uncaught.uncaughtException(Thread.currentThread(), t));
    }

    public static Executor newParasiticExecutor(final Thread.UncaughtExceptionHandler uncaught) {
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

    interface Batcher {
        void batch(final Runnable command);
    }

    public static abstract class BatchingExecutor implements Executor, Batcher, AutoCloseable {
        // this number might be tunable based on ForkJoinPool
        private static final int RUN_LIMIT = 1024;

        abstract class Batch implements ForkJoinPool.ManagedBlocker, Blocking.Blocker, Batcher, Runnable {
            // TODO: this can be optimized by unboxing the first Runnable
            final List<Runnable> tasks;

            Blocking.Blocker parentBlocker = MISSING_PARENT_BLOCKER;

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
                    (!(command instanceof DefaultPromise.Transformation) || ((DefaultPromise.Transformation<?, ?>) command).benefitsFromBatching());
        }
    }
}
