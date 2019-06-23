package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

class SyncBatch extends AbstractBatch<SyncBatchingScheduler> implements Runnable {

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
