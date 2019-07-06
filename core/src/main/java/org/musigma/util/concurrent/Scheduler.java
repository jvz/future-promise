package org.musigma.util.concurrent;

public interface Scheduler {

    static Scheduler common() {
        return Schedulers.COMMON;
    }

    static Scheduler parasitic() {
        return Schedulers.PARASITIC;
    }

    void execute(final Runnable runnable);

    void reportFailure(final Throwable t);

}
