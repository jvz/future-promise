package org.musigma.util.concurrent.impl;

import org.apiguardian.api.API;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

public final class Blocking {

    private Blocking() {
    }

    /**
     * Wraps a Callable to indicate that its underlying computation may cause blocking.
     * This is used to help improve performance and avoid deadlocking when executing blocking code asynchronously.
     */
    @API(status = API.Status.EXPERIMENTAL)
    public static <T> Callable<T> blocking(final Callable<T> callable) {
        return () -> Blocker.current().blockOn(callable);
    }

    interface Blocker {
        <T> T blockOn(final Callable<T> callable) throws Exception;

        default <T> T using(final UncheckedFunction<? super Blocker, ? extends T> function) throws Exception {
            Blocker previous = CURRENT.get();
            if (previous == this) {
                return function.apply(prefer(this));
            }
            CURRENT.set(this);
            try {
                return function.apply(prefer(previous));
            } finally {
                CURRENT.set(previous);
            }
        }

        static Blocker prefer(final Blocker previous) {
            if (previous != null) {
                return previous;
            }
            Thread current = Thread.currentThread();
            return current instanceof Blocker ? (Blocker) current : DEFAULT;
        }

        static Blocker current() {
            return prefer(CURRENT.get());
        }
    }

    private static final ThreadLocal<Blocker> CURRENT = new ThreadLocal<>();
    private static final Blocker DEFAULT = Callable::call;

    static class ManagedThunk<T> implements ForkJoinPool.ManagedBlocker, Callable<T> {
        private final Callable<T> thunk;
        private Thunk<T> result;
        private boolean done;

        ManagedThunk(final Callable<T> thunk) {
            this.thunk = thunk;
        }

        @Override
        public boolean block() throws InterruptedException {
            if (!done) {
                result = Thunk.from(thunk);
                done = true;
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            return done;
        }

        @Override
        public T call() throws Exception {
            ForkJoinPool.managedBlock(this);
            return result.call();
        }
    }

    static class BlockingThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory {

        private class BlockingThread extends ForkJoinWorkerThread implements Blocker {
            private boolean blocked;

            private BlockingThread(final ForkJoinPool pool) {
                super(pool);
            }

            @Override
            public <T> T blockOn(final Callable<T> callable) throws Exception {
                if (Thread.currentThread() == this && !blocked && blockerPermits.tryAcquire()) {
                    blocked = true;
                    try {
                        return new ManagedThunk<>(callable).call();
                    } finally {
                        blocked = false;
                        blockerPermits.release();
                    }
                } else {
                    // unmanaged blocking
                    return callable.call();
                }
            }
        }

        private final boolean daemonic;
        private final Semaphore blockerPermits;
        private final String prefix;
        private final Thread.UncaughtExceptionHandler uncaught;

        BlockingThreadFactory(final boolean daemonic, final int maxBlockers, final String prefix, final Thread.UncaughtExceptionHandler uncaught) {
            this.daemonic = daemonic;
            if (maxBlockers < 0) throw new IllegalArgumentException("maxBlockers must be non-negative");
            this.blockerPermits = new Semaphore(maxBlockers);
            this.prefix = Objects.requireNonNull(prefix);
            this.uncaught = uncaught;
        }

        private <T extends Thread> T wire(final T thread) {
            thread.setDaemon(daemonic);
            thread.setUncaughtExceptionHandler(uncaught);
            thread.setName(prefix + '-' + thread.getId());
            return thread;
        }

        @Override
        public ForkJoinWorkerThread newThread(final ForkJoinPool pool) {
            return wire(new BlockingThread(pool));
        }

        @Override
        public Thread newThread(final Runnable r) {
            return wire(new Thread(r));
        }
    }

}
