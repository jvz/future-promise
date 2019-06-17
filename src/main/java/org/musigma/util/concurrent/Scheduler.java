package org.musigma.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public interface Scheduler extends Executor {

    static Scheduler common() {
        return SchedulerAdapter.fromForkJoinPool(ForkJoinPool.commonPool());
    }

    static Scheduler inline() {
        return new SchedulerAdapter(Runnable::run, Throwable::printStackTrace);
    }

    void handleException(final Throwable t);
}
