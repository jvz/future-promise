package org.musigma.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

class SchedulerAdapter implements SchedulerExecutor {

    private final Executor executor;
    private final Consumer<Throwable> exceptionHandler;

    static SchedulerAdapter fromForkJoinPool(final ForkJoinPool pool) {
        return new SchedulerAdapter(pool, t -> pool.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t));
    }

    SchedulerAdapter(final Executor executor, final Consumer<Throwable> exceptionHandler) {
        this.executor = executor;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void execute(final Runnable command) {
        executor.execute(command);
    }

    @Override
    public void reportError(final Throwable t) {
        exceptionHandler.accept(t);
    }
}
