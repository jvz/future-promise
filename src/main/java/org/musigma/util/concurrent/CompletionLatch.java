package org.musigma.util.concurrent;

import org.musigma.util.Try;
import org.musigma.util.function.Consumer;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

class CompletionLatch<T> extends AbstractQueuedSynchronizer implements Consumer<Try<T>> {
    // avoid using volatile by using acquire/release
    private Try<T> result;

    Try<T> getResult() {
        return result;
    }

    @Override
    protected int tryAcquireShared(final int arg) {
        return getState() != 0 ? 1 : -1;
    }

    @Override
    protected boolean tryReleaseShared(final int arg) {
        setState(1);
        return true;
    }

    @Override
    public void accept(final Try<T> value) {
        result = value;
        releaseShared(1);
    }
}
