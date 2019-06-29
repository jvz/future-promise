package org.musigma.util.concurrent;

import org.musigma.util.Thunk;

import java.util.concurrent.Callable;

public interface Promise<T> {

    static <T> Promise<T> newPromise() {
        return new DefaultPromise<>();
    }

    static <T> Promise<T> from(final Callable<T> result) {
        return new DefaultPromise<>(result);
    }

    static <T> Promise<T> successful(final T result) {
        return from(Thunk.value(result));
    }

    static <T> Promise<T> failed(final Throwable throwable) {
        return from(Thunk.error(throwable));
    }

    Future<T> future();

    boolean isDone();

    boolean tryComplete(final Callable<T> result);

    default Promise<T> complete(final Callable<T> result) {
        if (tryComplete(result)) {
            return this;
        } else {
            throw new IllegalStateException("cannot complete a promise that is done already");
        }
    }

    default void completeWith(final Future<T> other) {
        if (future() != other) {
            other.onComplete(this::tryComplete, Scheduler.parasitic());
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
