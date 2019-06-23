package org.musigma.util.concurrent;

class ParasiticScheduler implements SchedulerExecutor, SyncBatchingScheduler {

    private final ThreadLocal<Object> syncContext = new ThreadLocal<>();

    ParasiticScheduler() {
    }

    @Override
    public int getPreBatchTaskCount() {
        Object o = syncContext.get();
        return o instanceof Integer ? (Integer) o : 0;
    }

    @Override
    public void setPreBatchTaskCount(final int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }
        if (count == 0) {
            syncContext.remove();
        } else {
            syncContext.set(count);
        }
    }

    @Override
    public SyncBatch getCurrentBatch() {
        Object o = syncContext.get();
        return o instanceof SyncBatch ? (SyncBatch) o : null;
    }

    @Override
    public void setCurrentBatch(final SyncBatch batch) {
        syncContext.set(batch);
    }

    @Override
    public void clearCurrentBatch() {
        syncContext.remove();
    }

    @Override
    public void submitForExecution(final Runnable runnable) {
        runnable.run();
    }

    @Override
    public void execute(final Runnable runnable) {
        submitSyncBatched(runnable);
    }

    @Override
    public void reportFailure(final Throwable t) {
        t.printStackTrace();
    }

}
