package org.musigma.util.concurrent;

interface BatchingScheduler<BatchT extends AbstractBatch> extends Scheduler {
    BatchT getCurrentBatch();

    void setCurrentBatch(final BatchT batch);

    void clearCurrentBatch();

    void submitForExecution(final Runnable runnable);
}
