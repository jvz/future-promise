package org.musigma.util.concurrent;

import org.musigma.util.control.Exceptions;
import org.musigma.util.function.UncheckedFunction;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

class AsyncBatch extends AbstractBatch<AsyncBatchingScheduler> implements Runnable, BlockContext, UncheckedFunction<BlockContext, Throwable> {

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
