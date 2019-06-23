package org.musigma.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface SchedulerExecutorService extends SchedulerExecutor, ExecutorService {
    static SchedulerExecutorService fromExecutorService(final ExecutorService es, final Consumer<Throwable> reporter) {
        return es == null ? DefaultSchedulerExecutorService.createDefaultSchedulerExecutorService(reporter) :
                new ExecutorServiceAdapter(es, reporter);
    }
}
