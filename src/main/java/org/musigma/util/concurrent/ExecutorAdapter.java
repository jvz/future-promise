package org.musigma.util.concurrent;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

class ExecutorAdapter implements SchedulerExecutor {

    private final Executor executor;
    private final Consumer<Throwable> reporter;

    ExecutorAdapter(final Executor executor, final Consumer<Throwable> reporter) {
        this.executor = executor;
        this.reporter = reporter;
    }

    @Override
    public void execute(final Runnable command) {
        executor.execute(command);
    }

    @Override
    public void reportFailure(final Throwable t) {
        reporter.accept(t);
    }
}
