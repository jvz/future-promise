package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

class SyncBatch extends AbstractBatch implements Runnable {

    SyncBatch(final BatchingScheduler scheduler, final Runnable runnable) {
        super(scheduler, runnable);
    }

    @Override
    public void run() {
        while (isBlocking()) {
            try {
                runN(BatchingSchedulers.RUN_LIMIT);
            } catch (final Throwable throwable) {
                Exceptions.rethrowIfFatal(throwable);
                scheduler.reportFailure(throwable);
            }
        }
    }
}
