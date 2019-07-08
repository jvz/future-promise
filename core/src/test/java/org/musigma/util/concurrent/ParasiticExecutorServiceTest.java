package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ParasiticExecutorServiceTest {

    private static class NoStackTrace extends RuntimeException {
        private NoStackTrace() {
            super("do not rethrow", null, false, false);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    @Test
    void shouldRunOnCallingThread() {
        Thread t = Thread.currentThread();
        AtomicReference<Thread> tRef = new AtomicReference<>();
        Executors.parasitic().execute(() -> tRef.set(Thread.currentThread()));
        assertSame(t, tRef.get());
    }

    @Test
    void shouldNotRethrowNonFatalExceptions() {
        Executors.parasitic().execute(() -> {
            throw new NoStackTrace();
        });
    }

    @Test
    void shouldRethrowFatalExceptions() {
        OutOfMemoryError error = new OutOfMemoryError("test");
        assertThrows(OutOfMemoryError.class, () -> Executors.parasitic().execute(() -> {
            throw error;
        }));
    }

    @Test
    void shouldContinueAfterNonFatalException() {
        AtomicReference<String> value = new AtomicReference<>();
        Executors.parasitic().execute(() -> {
            throw new NoStackTrace();
        });
        Executors.parasitic().execute(() -> value.set("hello world"));
        assertEquals("hello world", value.get());
    }

    @Test
    void shouldNotOverflowStack() {
        recurse(100000);
    }

    private void recurse(final int i) {
        if (i > 0) {
            Executors.parasitic().execute(() -> recurse(i - 1));
        }
    }

}