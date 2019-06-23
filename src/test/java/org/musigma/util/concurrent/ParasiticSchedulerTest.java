package org.musigma.util.concurrent;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class ParasiticSchedulerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldRunOnCallingThread() {
        Thread t = Thread.currentThread();
        AtomicReference<Thread> tRef = new AtomicReference<>();
        Scheduler.parasitic().execute(() -> tRef.set(Thread.currentThread()));
        assertSame(t, tRef.get());
    }

    @Test
    public void shouldNotRethrowNonFatalExceptions() {
        Scheduler.parasitic().execute(() -> {
            throw new RuntimeException("do not rethrow");
        });
    }

    @Test
    public void shouldRethrowFatalExceptions() {
        OutOfMemoryError error = new OutOfMemoryError("test");
        expectedException.expect(equalTo(error));
        Scheduler.parasitic().execute(() -> {
            throw error;
        });
    }

    @Test
    public void shouldContinueAfterNonFatalException() {
        AtomicReference<String> value = new AtomicReference<>();
        Scheduler.parasitic().execute(() -> {
            throw new RuntimeException("do not rethrow");
        });
        Scheduler.parasitic().execute(() -> value.set("hello world"));
        assertEquals("hello world", value.get());
    }

    @Test
    public void shouldNotOverflowStack() {
        recurse(100000);
    }

    private void recurse(final int i) {
        if (i > 0) {
            Scheduler.parasitic().execute(() -> recurse(i - 1));
        }
    }

}