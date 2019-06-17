package org.musigma.util.concurrent;

import org.musigma.util.Failure;
import org.musigma.util.Success;
import org.musigma.util.Try;

public interface Promise<T> {
    static <T> Promise<T> newPromise() {
        return new impl.DefaultPromise<>(null);
    }

    static <T> Promise<T> fromTry(final Try<T> result) {
        return new impl.DefaultPromise<>(result);
    }

    static <T> Promise<T> successful(final T result) {
        return fromTry(new Success<>(result));
    }

    static <T> Promise<T> failed(final Throwable throwable) {
        return fromTry(new Failure<>(throwable));
    }

    Future<T> future();

    boolean isDone();

    boolean tryComplete(final Try<T> result);

    default Promise<T> complete(final Try<T> result) {
        if (tryComplete(result)) {
            return this;
        } else {
            throw new IllegalStateException("cannot complete a promise that is done already");
        }
    }

    default Promise<T> completeWith(final Future<T> other) {
        if (future() != other) {
            other.onComplete(this::tryComplete, Scheduler.inline());
        }
        return this;
    }

    default Promise<T> success(final T value) {
        return complete(new Success<>(value));
    }

    default boolean trySuccess(final T value) {
        return tryComplete(new Success<>(value));
    }

    default Promise<T> failure(final Throwable throwable) {
        return complete(new Failure<>(throwable));
    }

    default boolean tryFailure(final Throwable throwable) {
        return tryComplete(new Failure<>(throwable));
    }
}
