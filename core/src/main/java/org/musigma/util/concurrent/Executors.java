package org.musigma.util.concurrent;

import java.util.concurrent.Executor;

public final class Executors {

    private Executors() {
    }

    private static final Thread.UncaughtExceptionHandler DEFAULT_HANDLER = (t, e) -> e.printStackTrace();
    private static final Executor COMMON = Blocking.newGlobalExecutor(DEFAULT_HANDLER);
    private static final Executor PARASITIC = Blocking.newParasiticExecutor(DEFAULT_HANDLER);

    public static Executor common() {
        return COMMON;
    }

    public static Executor parasitic() {
        return PARASITIC;
    }

}
