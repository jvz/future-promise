package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.Consumer;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

class CompletionLatch<T> extends AbstractQueuedSynchronizer implements Consumer<Thunk<T>> {
    // avoid using volatile by using acquire/release
    private Thunk<T> result;

    Thunk<T> getResult() {
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
    public void accept(final Thunk<T> value) {
        result = value;
        releaseShared(1);
    }
}
