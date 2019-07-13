package org.musigma.util.concurrent;

import java.util.concurrent.Executor;

public final class Executors {

    private Executors() {
    }

    private static final Executor COMMON = Batching.createDefaultExecutor(Throwable::printStackTrace);
    private static final Executor PARASITIC = new Batching.ParasiticExecutor();

    public static Executor common() {
        return COMMON;
    }

    public static Executor parasitic() {
        return PARASITIC;
    }

}
