package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertSame;

class DefaultSchedulerExecutorServiceTest {

    @Test
    void testDefaultSchedulerReportsUncaughtExceptions() throws ExecutionException, InterruptedException {
        Promise<Throwable> p = Promise.newPromise();
        SchedulerExecutorService ses = SchedulerExecutorService.fromExecutorService(null, p::trySuccess);
        RuntimeException e = new RuntimeException();
        try {
            ses.execute(() -> {
                throw e;
            });
            assertSame(e, p.future().get());
        } finally {
            ses.shutdown();
        }
    }

}