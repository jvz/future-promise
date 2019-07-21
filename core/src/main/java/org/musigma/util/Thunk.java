package org.musigma.util;

import org.apiguardian.api.API;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Reified result of a Callable. Provides a monadic API over success and error values.
 * This class is comparable to {@code scala.util.Try}.
 *
 * @param <T> return type of this thunk
 */
@API(status = API.Status.EXPERIMENTAL)
public final class Thunk<T> implements Callable<T> {

    private final Exception error;
    private final T value;

    private Thunk(final Exception error) {
        this.error = error;
        this.value = null;
    }

    private Thunk(final T value) {
        this.error = null;
        this.value = value;
    }

    /**
     * Reifies the result of the provided Callable. If the given callable is not already a Thunk, it will be
     * executed and reified into a Thunk.
     *
     * @param callable potentially deferrable value to reify
     * @param <T>      type of result when successful
     * @return the callable reified into a Thunk
     */
    public static <T> Thunk<T> from(final Callable<T> callable) {
        Objects.requireNonNull(callable);
        if (callable instanceof Thunk) {
            return (Thunk<T>) callable;
        }
        try {
            return value(callable.call());
        } catch (final Exception e) {
            return error(e);
        }
    }

    /**
     * Creates a successful Thunk from a given value.
     *
     * @param value success value (can be null)
     * @param <T>   type of success value
     * @return the success value reified into a Thunk
     */
    public static <T> Thunk<T> value(final T value) {
        return new Thunk<>(value);
    }

    /**
     * Creates a failed Thunk from a given Exception.
     *
     * @param error error value (cannot be null)
     * @param <T>   type of success value
     * @return the error value reified into a Thunk
     */
    public static <T> Thunk<T> error(final Exception error) {
        return new Thunk<>(Objects.requireNonNull(error));
    }

    /**
     * Returns this success value or throws the error value.
     *
     * @return this success value if successful
     * @throws Exception this error if failure
     */
    @Override
    public T call() throws Exception {
        if (isError()) {
            throw error;
        }
        return value;
    }

    /**
     * Returns this success value or throws if this is an error value.
     *
     * @return this success value if successful
     * @throws IllegalStateException if this is an error value
     */
    public T value() {
        if (isError()) {
            throw new IllegalStateException("Cannot call Thunk::value on an error");
        }
        return value;
    }

    /**
     * Returns this error value or throws if this is a success value.
     *
     * @return this error
     * @throws IllegalStateException if this is a success value
     */
    public Exception error() {
        if (!isError()) {
            throw new IllegalStateException("Cannot call Thunk::error on a value");
        }
        return error;
    }

    /**
     * Indicates if this contains an error value.
     *
     * @return true if this has an error
     */
    public boolean isError() {
        return error != null;
    }

    /**
     * Indicates if this contains a success value. Note that {@code null} is a valid success value, so this
     * logically means that there is no error.
     *
     * @return true if this has a success value
     */
    public boolean isSuccess() {
        return error == null;
    }

    @SuppressWarnings("unchecked")
    public <U> Thunk<U> recast() {
        return (Thunk<U>) this;
    }

    public <U> Thunk<U> recastIfError() {
        if (isError()) {
            return recast();
        } else {
            throw new ClassCastException("cannot recast a non-error");
        }
    }

    public <U> Thunk<U> map(final UncheckedFunction<? super T, ? extends U> function) {
        if (isError()) {
            return recast();
        }
        return from(() -> function.apply(value));
    }

    public <U> Thunk<U> flatMap(final UncheckedFunction<? super T, ? extends Callable<U>> function) {
        if (isError()) {
            return recast();
        }
        try {
            return from(function.apply(value));
        } catch (final Exception e) {
            return error(e);
        }
    }

    public Thunk<T> filter(final UncheckedPredicate<? super T> predicate) {
        if (isError()) {
            return this;
        }
        try {
            return predicate.test(value) ? this : error(new NoSuchElementException("predicate failed for " + value));
        } catch (final Exception e) {
            return error(e);
        }
    }

    public <U> Thunk<U> transformWith(final UncheckedFunction<? super T, ? extends Callable<U>> ifSuccess,
                                      final UncheckedFunction<Exception, ? extends Callable<U>> ifError) {
        try {
            return from(isSuccess() ? ifSuccess.apply(value) : ifError.apply(error));
        } catch (final Exception e) {
            return error(e);
        }
    }

    public Thunk<T> recover(final UncheckedFunction<Exception, ? extends T> function) {
        try {
            return isSuccess() ? this : value(function.apply(error));
        } catch (final Exception e) {
            return error(e);
        }
    }

    public Thunk<T> recoverWith(final UncheckedFunction<Exception, ? extends Callable<T>> function) {
        try {
            return isSuccess() ? this : from(function.apply(error));
        } catch (final Exception e) {
            return error(e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Thunk<?> thunk = (Thunk<?>) o;
        return Objects.equals(error, thunk.error) &&
                Objects.equals(value, thunk.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, value);
    }

    @Override
    public String toString() {
        return isError() ? "Error{" + error + '}' : "Value{" + value + '}';
    }

}
