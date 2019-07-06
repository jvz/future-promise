package org.musigma.util.concurrent;

import java.util.Objects;

interface AsyncBatchingScheduler extends BatchingScheduler<AsyncBatch> {

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
