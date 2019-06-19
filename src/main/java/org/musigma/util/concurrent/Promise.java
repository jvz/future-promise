package org.musigma.util.concurrent;

import org.musigma.util.Thunk;

public interface Promise<T> {
    static <T> Promise<T> newPromise() {
        return new DefaultPromise<>(null);
    }

    static <T> Promise<T> fromThunk(final Thunk<T> result) {
        return new DefaultPromise<>(result);
    }

    static <T> Promise<T> successful(final T result) {
        return fromThunk(Thunk.value(result));
    }

    static <T> Promise<T> failed(final Throwable throwable) {
        return fromThunk(Thunk.error(throwable));
    }

    Future<T> future();

    boolean isDone();

    boolean tryComplete(final Thunk<T> result);

    default Promise<T> complete(final Thunk<T> result) {
        if (tryComplete(result)) {
            return this;
        } else {
            throw new IllegalStateException("cannot complete a promise that is done already");
        }
    }

    default void completeWith(final Future<T> other) {
        if (future() != other) {
            other.onComplete(this::tryComplete, Scheduler.inline());
        }
    }

    default Promise<T> success(final T value) {
        return complete(Thunk.value(value));
    }

    default boolean trySuccess(final T value) {
        return tryComplete(Thunk.value(value));
    }

    default Promise<T> failure(final Throwable throwable) {
        return complete(Thunk.error(throwable));
    }

    default boolean tryFailure(final Throwable throwable) {
        return tryComplete(Thunk.error(throwable));
    }
}
