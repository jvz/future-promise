package org.musigma.util;

/**
 * Exception utilities for dealing with checked and fatal exceptions.
 */
public final class Exceptions {

    private Exceptions() {
    }

    /**
     * Rethrows the given Throwable without being a checked exception or wrapping.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrow(final Throwable t) throws T {
        throw (T) t;
    }

    /**
     * Indicates if the given Throwable is fatal.
     *
     * @see VirtualMachineError
     * @see ThreadDeath
     * @see InterruptedException
     * @see LinkageError
     */
    public static boolean isFatal(final Throwable t) {
        return t instanceof VirtualMachineError || t instanceof ThreadDeath || t instanceof InterruptedException || t instanceof LinkageError;
    }

    /**
     * Rethrows the given exception only if it's a fatal exception.
     */
    public static void rethrowIfFatal(final Throwable t) {
        if (isFatal(t)) {
            rethrow(t);
        }
    }
}
