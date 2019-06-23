package org.musigma.util.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class AbstractBatch {
    final BatchingExecutor executor;
    // TODO: this can be optimized by unboxing the first Runnable
    final List<Runnable> runnables;

    AbstractBatch(final BatchingExecutor executor, final Runnable runnable) {
        this(executor, Collections.singletonList(runnable));
    }

    AbstractBatch(final BatchingExecutor executor, final List<Runnable> runnables) {
        this.executor = executor;
        this.runnables = new ArrayList<>(runnables);
    }

    boolean isBlocking() {
        return runnables.size() > 0;
    }

    void push(final Runnable r) {
        runnables.add(r);
    }

    void runN(final int n) {
        if (n < 0) {
            throw new IllegalArgumentException("n must be non-negative");
        }
        for (int i = 0; i < n && i < runnables.size(); i++) {
            runnables.remove(0).run();
        }
    }
}
