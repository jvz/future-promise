package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

import java.util.Objects;
import java.util.concurrent.Executor;

interface BatchingExecutor extends Executor {
    // FIXME: refactor into method
    ThreadLocal<Object> LOCAL_TASKS = new ThreadLocal<>();

    void submitForExecution(final Runnable runnable);

    void reportFailure(final Throwable throwable);

    default void submitAsyncBatched(final Runnable runnable) {
        Objects.requireNonNull(runnable);
        Object o = LOCAL_TASKS.get();
        if (o instanceof AsyncBatch) {
            ((AsyncBatch) o).push(runnable);
        } else {
            submitForExecution(new AsyncBatch(this, runnable));
        }
    }

    default void submitSyncBatched(final Runnable runnable) {
        Objects.requireNonNull(runnable);
        Object b = LOCAL_TASKS.get();
        if (b instanceof SyncBatch) {
            ((SyncBatch) b).push(runnable);
        } else {
            final int i = b instanceof Integer ? (Integer) b : 0;
            if (i < BatchingExecutors.SYNC_PRE_BATCH_DEPTH) {
                LOCAL_TASKS.set(i + 1);
                try {
                    submitForExecution(runnable);
                } catch (final Throwable throwable) {
                    Exceptions.rethrowIfFatal(throwable);
                    reportFailure(throwable);
                } finally {
                    LOCAL_TASKS.set(b);
                }
            } else {
                SyncBatch batch = new SyncBatch(this, runnable);
                LOCAL_TASKS.set(batch);
                try {
                    submitForExecution(batch);
                } finally {
                    LOCAL_TASKS.set(b);
                }
            }
        }
    }
}
