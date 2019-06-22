package org.musigma.util.concurrent;

import java.util.concurrent.ForkJoinPool;

public interface Scheduler {

    static Scheduler common() {
        return SchedulerAdapter.fromForkJoinPool(ForkJoinPool.commonPool());
    }

    static Scheduler parasitic() {
        return new SchedulerAdapter(Runnable::run, Throwable::printStackTrace);
    }

    void execute(final Runnable runnable);

    void reportError(final Throwable t);

}
