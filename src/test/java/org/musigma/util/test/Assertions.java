package org.musigma.util.test;

import org.junit.jupiter.api.function.Executable;
import org.musigma.util.Exceptions;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class Assertions {
    public static void assertThrowsWrapped(final Class<? extends Exception> wrappedType, final Executable executable, final String errorMessage) {
        try {
            executable.execute();
        } catch (final ExecutionException wrapper) {
            final Throwable wrapped = wrapper.getCause();
            assertAll(
                    () -> assertNotNull(wrapped),
                    () -> assertEquals(wrappedType, wrapped.getClass()),
                    () -> assertEquals(errorMessage, wrapped.getMessage()));
        } catch (final Throwable t) {
            Exceptions.rethrowIfFatal(t);
            throw new AssertionFailedError("unexpected exception type", ExecutionException.class, t.getClass(), t);
        }
    }
}
