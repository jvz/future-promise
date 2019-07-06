package org.musigma.util.concurrent;

final class Schedulers {

    static final int SYNC_PRE_BATCH_DEPTH = 16;
    static final int RUN_LIMIT = 1024;
    static final Scheduler COMMON = SchedulerExecutor.fromExecutor(null, Throwable::printStackTrace);
    static final Scheduler PARASITIC = new ParasiticScheduler();

    private Schedulers() {
    }

}
