package org.musigma.util.control;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ControlThrowableTest {

    private static class Ctrl extends ControlThrowable {}

    @Test
    void shouldHaveEmptyStackTraceWhenThrown() {
        try {
            throw new Ctrl();
        } catch (final Ctrl c) {
            assertEquals(0, c.getStackTrace().length);
        }
    }

    @Test
    void shouldBeConsideredFatal() {
        Ctrl c = new Ctrl();
        assertTrue(Exceptions.isFatal(c));
        assertThrows(Ctrl.class, () -> Exceptions.rethrowIfFatal(c));
    }

    @Test
    void shouldNotAddSuppressedExceptions() {
        try {
            Ctrl c = new Ctrl();
            c.addSuppressed(new Throwable("should not be suppressed"));
            throw c;
        } catch (final Ctrl c) {
            assertEquals(0, c.getSuppressed().length);
        }
    }
}