package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.function.UncheckedFunction;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

class AsyncBatch extends AbstractBatch implements Runnable, BlockContext, UncheckedFunction<BlockContext, Throwable> {

    private BlockContext parentBlockContext = BatchingSchedulers.MISSING_PARENT_BLOCK_CONTEXT;

    AsyncBatch(final BatchingScheduler scheduler, final Runnable runnable) {
        super(scheduler, runnable);
    }

    AsyncBatch(final BatchingScheduler scheduler, final List<Runnable> runnables) {
        super(scheduler, runnables);
    }

    @Override
    public void run() {
        BatchingScheduler.LOCAL_TASKS.set(this); // later cleared in apply()
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
            runN(BatchingSchedulers.RUN_LIMIT);
            return null;
        } catch (final Throwable throwable) {
            return throwable;
        } finally {
            parentBlockContext = BatchingSchedulers.MISSING_PARENT_BLOCK_CONTEXT;
            BatchingScheduler.LOCAL_TASKS.remove();
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
