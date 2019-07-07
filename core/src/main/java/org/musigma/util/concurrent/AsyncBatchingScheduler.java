package org.musigma.util.concurrent;

import java.util.Objects;

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

    @Override
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
