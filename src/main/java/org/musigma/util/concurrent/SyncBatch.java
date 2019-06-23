package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

class SyncBatch extends AbstractBatch implements Runnable {

    SyncBatch(final BatchingExecutor executor, final Runnable runnable) {
        super(executor, runnable);
    }

    @Override
    public void run() {
        while (isBlocking()) {
            try {
                runN(BatchingExecutors.RUN_LIMIT);
            } catch (final Throwable throwable) {
                Exceptions.rethrowIfFatal(throwable);
                executor.reportFailure(throwable);
            }
        }
    }
}
