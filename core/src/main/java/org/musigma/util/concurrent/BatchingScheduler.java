package org.musigma.util.concurrent;

interface BatchingScheduler<BatchT extends AbstractBatch> extends Scheduler {
    // synchronous and asynchronous batching based on:
    // https://github.com/scala/scala/pull/7663

    BatchT getCurrentBatch();

    void setCurrentBatch(final BatchT batch);

    void clearCurrentBatch();

    void submitForExecution(final Runnable runnable);

}
