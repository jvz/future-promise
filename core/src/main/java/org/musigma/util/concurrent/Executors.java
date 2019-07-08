package org.musigma.util.concurrent;

import java.util.concurrent.ExecutorService;

public final class Executors {

    private Executors() {
    }

    private static final ExecutorService COMMON = Batching.createDefaultExecutor(Throwable::printStackTrace);
    private static final ExecutorService PARASITIC = new Batching.ParasiticExecutorService();

    public static ExecutorService common() {
        return COMMON;
    }

    public static ExecutorService parasitic() {
        return PARASITIC;
    }

}
