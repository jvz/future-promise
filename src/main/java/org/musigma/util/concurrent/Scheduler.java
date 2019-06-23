package org.musigma.util.concurrent;

public interface Scheduler {

    static Scheduler common() {
        return SchedulerExecutor.fromExecutor(null, Throwable::printStackTrace);
    }

    static Scheduler parasitic() {
        return new ExecutorAdapter(Runnable::run, Throwable::printStackTrace);
    }

    void execute(final Runnable runnable);

    void reportFailure(final Throwable t);

}
