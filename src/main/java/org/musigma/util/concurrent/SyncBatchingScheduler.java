package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

import java.util.Objects;

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
