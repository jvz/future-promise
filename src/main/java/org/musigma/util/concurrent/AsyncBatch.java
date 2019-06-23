package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;

import java.util.List;
import java.util.concurrent.ExecutionException;

class AsyncBatch extends AbstractBatch implements Runnable, BlockContext, UncheckedFunction<BlockContext, Throwable> {

    private BlockContext parentBlockContext = BatchingExecutors.MISSING_PARENT_BLOCK_CONTEXT;

    AsyncBatch(final BatchingExecutor executor, final Runnable runnable) {
        super(executor, runnable);
    }

    AsyncBatch(final BatchingExecutor executor, final List<Runnable> runnables) {
        super(executor, runnables);
    }

    @Override
    public void run() {
        BatchingExecutor.LOCAL_TASKS.set(this); // later cleared in apply()
        Throwable failure;
        try {
            failure = resubmit(BlockContexts.usingBlockContext(this, this));
        } catch (final Throwable throwable) {
            failure = throwable;
        }
        if (failure != null) {
            Exceptions.rethrow(failure);
        }
    }

    @Override
    public <T> T blockOn(final Thunk<T> thunk) throws Throwable {
        if (isBlocking()) {
            executor.submitForExecution(cloneAndClear());
        }
        return parentBlockContext.blockOn(thunk);
    }

    @Override
    public Throwable apply(final BlockContext value) throws Throwable {
        try {
            parentBlockContext = value;
            runN(BatchingExecutors.RUN_LIMIT);
            return null;
        } catch (final Throwable throwable) {
            return throwable;
        } finally {
            parentBlockContext = BatchingExecutors.MISSING_PARENT_BLOCK_CONTEXT;
            BatchingExecutor.LOCAL_TASKS.remove();
        }
    }

    private Throwable resubmit(final Throwable throwable) {
        if (!isBlocking()) {
            return throwable;
        }
        try {
            executor.submitForExecution(this);
            return throwable;
        } catch (final Throwable t) {
            // FIXME: this may not be an appropriate place to rethrow?
            Exceptions.rethrowIfFatal(t);
            ExecutionException e = new ExecutionException("non-fatal error occurred and resubmission failed", throwable);
            e.addSuppressed(t);
            return e;
        }
    }

    private AsyncBatch cloneAndClear() {
        final AsyncBatch batch = new AsyncBatch(executor, runnables);
        runnables.clear();
        return batch;
    }
}
