package org.musigma.util.concurrent;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertSame;

public class DefaultSchedulerExecutorServiceTest {

    @Test
    public void testDefaultSchedulerReportsUncaughtExceptions() throws ExecutionException, InterruptedException {
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