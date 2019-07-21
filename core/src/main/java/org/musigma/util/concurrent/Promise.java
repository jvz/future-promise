package org.musigma.util.concurrent;

import org.apiguardian.api.API;
import org.musigma.util.Thunk;

import java.util.concurrent.Callable;

/**
 * Asynchronously-completable object holder for either a success value or an exception.
 * Consumers of this promise should obtain a {@link #future() Future} reference from this Promise.
 *
 * @param <T> type of success value
 */
@API(status = API.Status.EXPERIMENTAL)
public interface Promise<T> {

    /**
     * Creates a new incomplete Promise.
     *
     * @param <T> type of success value
     * @return a new incomplete Promise
     */
    static <T> Promise<T> newPromise() {
        return PromiseFactory.getInstance().newIncompletePromise();
    }

    /**
     * Creates a new completed Promise from the result of the given Callable.
     *
     * @param callable value or error provider to complete the Promise
     * @param <T>      type of success value
     * @return a new completed Promise from the result of the Callable
     */
    static <T> Promise<T> from(final Callable<T> callable) {
        return PromiseFactory.getInstance().newCompletePromise(callable);
    }

    /**
     * Creates a new completed Promise from a given success value.
     *
     * @param result success value
     * @param <T>    type of success value
     * @return a new completed Promise with the given success value
     */
    static <T> Promise<T> successful(final T result) {
        return from(Thunk.value(result));
    }

    /**
     * Creates a new completed Promise from a given Exception.
     *
     * @param e   exception value
     * @param <T> type of success value
     * @return a new completed Promise with the given Exception
     */
    static <T> Promise<T> failed(final Exception e) {
        return from(Thunk.error(e));
    }

    /**
     * Returns a Future corresponding to the completion of this Promise.
     *
     * @return a Future corresponding to this
     */
    Future<T> future();

    /**
     * Indicates if this Promise has been completed.
     *
     * @return true if this has a success value or exception, or false if this is still incomplete
     */
    boolean isDone();

    /**
     * Attempts to complete this Promise using the result of the given Callable only if this Promise is incomplete.
     *
     * @param callable value or error provider to complete this with
     * @return true if this was completed by the Callable or false if this was already completed
     */
    boolean tryComplete(final Callable<T> callable);

    /**
     * Completes this Promise using the result of the given Callable or errs if this is already completed.
     *
     * @param result value or error provider to complete this with
     * @return this
     * @throws IllegalArgumentException if this is already completed
     */
    default Promise<T> complete(final Callable<T> result) {
        if (tryComplete(result)) {
            return this;
        } else {
            throw new IllegalStateException("cannot complete a promise that is done already");
        }
    }

    /**
     * Attempts to complete this Promise using the result of another Future.
     *
     * @param other Future to complete this with
     */
    default void completeWith(final Future<T> other) {
        if (future() != other) {
            other.onComplete(Executors.parasitic(), this::tryComplete);
        }
    }

    /**
     * Completes this Promise using the given success value.
     *
     * @param value success value
     * @return this
     * @throws IllegalArgumentException if this is already completed
     */
    default Promise<T> success(final T value) {
        return complete(Thunk.value(value));
    }

    /**
     * Attempts to complete this Promise using the given success value.
     *
     * @param value success value
     * @return true if this was completed by the given value or false if this was already completed
     */
    default boolean trySuccess(final T value) {
        return tryComplete(Thunk.value(value));
    }

    /**
     * Completes this Promise using the given Exception.
     *
     * @param e exception value
     * @return this
     * @throws IllegalArgumentException if this is already completed
     */
    default Promise<T> failure(final Exception e) {
        return complete(Thunk.error(e));
    }

    /**
     * Attempts to complete this Promise using the given Exception.
     *
     * @param e exception value
     * @return true if this was completed by the given Exception or false if this was already completed
     */
    default boolean tryFailure(final Exception e) {
        return tryComplete(Thunk.error(e));
    }

}
