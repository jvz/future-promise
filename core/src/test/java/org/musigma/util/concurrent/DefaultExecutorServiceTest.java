package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertSame;

class DefaultExecutorServiceTest {

    @Test
    void testDefaultSchedulerReportsUncaughtExceptions() throws ExecutionException, InterruptedException {
        Promise<Throwable> p = Promise.newPromise();
        try (Blocking.BatchingExecutor executor = Blocking.newGlobalExecutor((t, e) -> p.trySuccess(e))) {
            RuntimeException e = new RuntimeException();
            executor.execute(() -> {
                throw e;
            });
            assertSame(e, p.future().get());
        }
    }

}