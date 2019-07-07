package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.function.UncheckedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

class Batching {

    interface BatchingScheduler<BatchT extends AbstractBatch> extends Scheduler {
        // synchronous and asynchronous batching based on:
        // https://github.com/scala/scala/pull/7663

        BatchT getCurrentBatch();

        void setCurrentBatch(final BatchT batch);

        void submitForExecution(final Runnable runnable);

    }

    abstract static class AbstractBatch<SchedulerT extends BatchingScheduler> {
        final SchedulerT scheduler;
        // TODO: this can be optimized by unboxing the first Runnable
        final List<Runnable> runnables;

        AbstractBatch(final SchedulerT scheduler, final Runnable runnable) {
            this(scheduler, Collections.singletonList(runnable));
        }

        AbstractBatch(final SchedulerT scheduler, final List<Runnable> runnables) {
            this.scheduler = scheduler;
            this.runnables = new ArrayList<>(runnables);
        }

        boolean isBlocking() {
            return runnables.size() > 0;
        }

        void push(final Runnable r) {
            runnables.add(r);
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

    interface AsyncBatchingScheduler extends BatchingScheduler<AsyncBatch> {

        ThreadLocal<AsyncBatch> asyncContext();

        @Override
        default AsyncBatch getCurrentBatch() {
            return asyncContext().get();
        }

        @Override
        default void setCurrentBatch(final AsyncBatch batch) {
            asyncContext().set(batch);
        }

        default void clearCurrentBatch() {
            asyncContext().remove();
        }

        default void submitAsyncBatched(final Runnable runnable) {
            Objects.requireNonNull(runnable);
            AsyncBatch batch = getCurrentBatch();
            if (batch != null) {
                batch.push(runnable);
            } else {
                submitForExecution(new AsyncBatch(this, runnable));
            }
        }

    }

    static class AsyncBatch extends AbstractBatch<AsyncBatchingScheduler> implements Runnable, BlockContext, UncheckedFunction<BlockContext, Throwable> {

        private static final BlockContext MISSING_PARENT_BLOCK_CONTEXT = new BlockContext() {
            @Override
            public <T> T blockOn(final Callable<T> thunk) throws Exception {
                throw new IllegalStateException("missing parent block context");
            }
        };

        private BlockContext parentBlockContext = MISSING_PARENT_BLOCK_CONTEXT;

        AsyncBatch(final AsyncBatchingScheduler scheduler, final Runnable runnable) {
            super(scheduler, runnable);
        }

        private AsyncBatch(final AsyncBatchingScheduler scheduler, final List<Runnable> runnables) {
            super(scheduler, runnables);
        }

        @Override
        public void run() {
            scheduler.setCurrentBatch(this); // later cleared in apply()
            Throwable failure;
            try {
                failure = resubmit(BlockContexts.usingBlockContext(this, this));
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
                scheduler.submitForExecution(cloneAndClear());
            }
            return parentBlockContext.blockOn(thunk);
        }

        @Override
        public Throwable apply(final BlockContext value) throws Exception {
            try {
                parentBlockContext = value;
                runN(Schedulers.RUN_LIMIT);
                return null;
            } catch (final Throwable throwable) {
                return throwable;
            } finally {
                parentBlockContext = MISSING_PARENT_BLOCK_CONTEXT;
                scheduler.clearCurrentBatch();
            }
        }

        private Throwable resubmit(final Throwable throwable) {
            if (!isBlocking()) {
                return throwable;
            }
            try {
                scheduler.submitForExecution(this);
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

        private AsyncBatch cloneAndClear() {
            final AsyncBatch batch = new AsyncBatch(scheduler, runnables);
            runnables.clear();
            return batch;
        }
    }

    interface SyncBatchingScheduler extends BatchingScheduler<SyncBatch> {

        int getPreBatchTaskCount();

        void setPreBatchTaskCount(final int count);

        default void submitSyncBatched(final Runnable runnable) {
            Objects.requireNonNull(runnable);
            SyncBatch currentBatch = getCurrentBatch();
            if (currentBatch != null) {
                currentBatch.push(runnable);
            } else {
                int i = getPreBatchTaskCount();
                try {
                    if (i < Schedulers.SYNC_PRE_BATCH_DEPTH) {
                        setPreBatchTaskCount(i + 1);
                        submitForExecution(runnable);
                    } else {
                        SyncBatch batch = new SyncBatch(this, runnable);
                        setCurrentBatch(batch);
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

    static class SyncBatch extends AbstractBatch<SyncBatchingScheduler> implements Runnable {

        SyncBatch(final SyncBatchingScheduler scheduler, final Runnable runnable) {
            super(scheduler, runnable);
        }

        @Override
        public void run() {
            while (isBlocking()) {
                try {
                    runN(Schedulers.RUN_LIMIT);
                } catch (final Throwable throwable) {
                    Exceptions.rethrowIfFatal(throwable);
                    scheduler.reportFailure(throwable);
                }
            }
        }

    }

}
