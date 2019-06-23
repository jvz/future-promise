package org.musigma.util.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

class DefaultSchedulerExecutorService extends ForkJoinPool implements SchedulerExecutorService, BatchingScheduler {

    private static String getString(final String propertyName, final String defaultValue) {
        try {
            return System.getProperty(propertyName, defaultValue);
        } catch (final SecurityException ignored) {
            return defaultValue;
        }
    }

    private static int getInt(final String propertyName, final String defaultValue) {
        final String s = getString(propertyName, defaultValue);
        if (s.charAt(0) == 'x') {
            return (int) Math.ceil(Double.parseDouble(s.substring(1)) * Runtime.getRuntime().availableProcessors());
        } else {
            return Integer.parseInt(s);
        }
    }

    static DefaultSchedulerExecutorService createDefaultSchedulerExecutorService(final Consumer<Throwable> reporter) {
        final int parallelism = Math.min(
                Math.max(getInt("org.musigma.util.concurrent.minThreads", "1"),
                        getInt("org.musigma.util.concurrent.numThreads", "x1")),
                getInt("org.musigma.util.concurrent.maxThreads", "x1"));
        final int maxBlockers = getInt("org.musigma.util.concurrent.maxExtraThreads", "256");
        final UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> reporter.accept(e);
        final DefaultThreadFactory threadFactory = new DefaultThreadFactory(true, maxBlockers, "global-scheduler", uncaughtExceptionHandler);
        return new DefaultSchedulerExecutorService(parallelism, threadFactory, uncaughtExceptionHandler);
    }

    private DefaultSchedulerExecutorService(final int parallelism, final ForkJoinWorkerThreadFactory factory, final UncaughtExceptionHandler handler) {
        super(parallelism, factory, handler, true);
    }

    @Override
    public void submitForExecution(final Runnable runnable) {
        super.execute(runnable);
    }

    @Override
    public void execute(final Runnable task) {
        if ((!(task instanceof Transformation) || ((Transformation<?, ?>) task).benefitsFromBatching()) && task instanceof Batchable) {
            submitAsyncBatched(task);
        } else {
            submitForExecution(task);
        }
    }

    @Override
    public void reportFailure(final Throwable t) {
        final UncaughtExceptionHandler handler = getUncaughtExceptionHandler();
        if (handler != null) {
            handler.uncaughtException(Thread.currentThread(), t);
        }
    }
}
