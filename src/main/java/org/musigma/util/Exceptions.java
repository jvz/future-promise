package org.musigma.util;

public final class Exceptions {

    private Exceptions() {
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrow(final Throwable t) throws T {
        throw (T) t;
    }

    public static void rethrowIfFatal(final Throwable t) {
        if (t instanceof VirtualMachineError || t instanceof ThreadDeath || t instanceof InterruptedException || t instanceof LinkageError) {
            rethrow(t);
        }
    }
}
