package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ParasiticSchedulerTest {

    @Test
    void shouldRunOnCallingThread() {
        Thread t = Thread.currentThread();
        AtomicReference<Thread> tRef = new AtomicReference<>();
        Scheduler.parasitic().execute(() -> tRef.set(Thread.currentThread()));
        assertSame(t, tRef.get());
    }

    @Test
    void shouldNotRethrowNonFatalExceptions() {
        Scheduler.parasitic().execute(() -> {
            throw new RuntimeException("do not rethrow");
        });
    }

    @Test
    void shouldRethrowFatalExceptions() {
        OutOfMemoryError error = new OutOfMemoryError("test");
        assertThrows(OutOfMemoryError.class, () -> Scheduler.parasitic().execute(() -> {
            throw error;
        }));
    }

    @Test
    void shouldContinueAfterNonFatalException() {
        AtomicReference<String> value = new AtomicReference<>();
        Scheduler.parasitic().execute(() -> {
            throw new RuntimeException("do not rethrow");
        });
        Scheduler.parasitic().execute(() -> value.set("hello world"));
        assertEquals("hello world", value.get());
    }

    @Test
    void shouldNotOverflowStack() {
        recurse(100000);
    }

    private void recurse(final int i) {
        if (i > 0) {
            Scheduler.parasitic().execute(() -> recurse(i - 1));
        }
    }

}