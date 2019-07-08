package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertSame;

class DefaultExecutorServiceTest {

    @Test
    void testDefaultSchedulerReportsUncaughtExceptions() throws ExecutionException, InterruptedException {
        Promise<Throwable> p = Promise.newPromise();
        ExecutorService es = Batching.createDefaultExecutor(p::trySuccess);
        RuntimeException e = new RuntimeException();
        try {
            es.execute(() -> {
                throw e;
            });
            assertSame(e, p.future().get());
        } finally {
            es.shutdown();
        }
    }

}