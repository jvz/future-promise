package org.musigma.util.concurrent;

class ParasiticScheduler implements SchedulerExecutor, BatchingScheduler {

    private static final ParasiticScheduler INSTANCE = new ParasiticScheduler();

    static ParasiticScheduler getInstance() {
        return INSTANCE;
    }

    private ParasiticScheduler() {
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
