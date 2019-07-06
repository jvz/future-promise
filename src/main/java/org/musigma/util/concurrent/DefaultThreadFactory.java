package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

class DefaultThreadFactory implements ThreadFactory, ForkJoinWorkerThreadFactory {

    private final boolean daemonic;
    private final String prefix;
    private final UncaughtExceptionHandler uncaught;
    private final Semaphore blockerPermits;

    DefaultThreadFactory(final boolean daemonic, final int maxBlockers, final String prefix, final UncaughtExceptionHandler exceptionHandler) {
        this.daemonic = daemonic;
        if (maxBlockers < 0) throw new IllegalArgumentException("maxBlockers must be non-negative");
        this.prefix = Objects.requireNonNull(prefix);
        this.uncaught = exceptionHandler;
        blockerPermits = new Semaphore(maxBlockers);
    }

    private <T extends Thread> T wire(final T thread) {
        thread.setDaemon(daemonic);
        thread.setUncaughtExceptionHandler(uncaught);
        thread.setName(prefix + '-' + thread.getId());
        return thread;
    }

    @Override
    public Thread newThread(final Runnable r) {
        return wire(new Thread(r));
    }

    @Override
    public ForkJoinWorkerThread newThread(final ForkJoinPool pool) {
        return wire(new DefaultForkJoinWorkerThread(pool));
    }

    private class DefaultForkJoinWorkerThread extends ForkJoinWorkerThread implements BlockContext {
        private boolean blocked;

        private DefaultForkJoinWorkerThread(final ForkJoinPool pool) {
            super(pool);
        }

        @Override
        public <T> T blockOn(final Callable<T> thunk) throws Exception {
            if (Thread.currentThread() == this && !blocked && blockerPermits.tryAcquire()) {
                try {
                    final ManagedBlockerThunk<T> blocker = new ManagedBlockerThunk<>(thunk);
                    blocked = true;
                    ForkJoinPool.managedBlock(blocker);
                    return blocker.result;
                } finally {
                    blocked = false;
                    blockerPermits.release();
                }
            } else {
                // unmanaged blocking
                return thunk.call();
            }
        }

        private class ManagedBlockerThunk<T> implements ManagedBlocker {
            private final Callable<T> thunk;
            private T result;
            private boolean done;

            private ManagedBlockerThunk(final Callable<T> thunk) {
                this.thunk = thunk;
            }

            @Override
            public boolean block() throws InterruptedException {
                if (!done) {
                    try {
                        result = thunk.call();
                        done = true;
                    } catch (final Throwable throwable) {
                        Exceptions.rethrowUnchecked(throwable);
                        return false;
                    }
                }
                return isReleasable();
            }

            @Override
            public boolean isReleasable() {
                return done;
            }
        }
    }

}
