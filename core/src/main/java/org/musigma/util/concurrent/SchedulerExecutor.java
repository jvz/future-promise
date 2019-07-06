package org.musigma.util.concurrent;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

public interface SchedulerExecutor extends Scheduler, Executor {

    static SchedulerExecutor fromExecutor(final Executor executor, final Consumer<Throwable> reporter) {
        return executor == null ? DefaultSchedulerExecutorService.createDefaultSchedulerExecutorService(reporter) :
                new ExecutorAdapter(executor, reporter);
    }

}
