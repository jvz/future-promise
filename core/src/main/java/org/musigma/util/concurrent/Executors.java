package org.musigma.util.concurrent;

import org.apiguardian.api.API;

import java.util.concurrent.Executor;

@API(status = API.Status.EXPERIMENTAL)
public final class Executors {

    private Executors() {
    }

    private static final Thread.UncaughtExceptionHandler DEFAULT_HANDLER = (t, e) -> e.printStackTrace();
    private static final Executor COMMON = Batching.newBatchingExecutor(DEFAULT_HANDLER);
    private static final Executor PARASITIC = Batching.newParasiticExecutor(DEFAULT_HANDLER);

    public static Executor common() {
        return COMMON;
    }

    public static Executor parasitic() {
        return PARASITIC;
    }

}
