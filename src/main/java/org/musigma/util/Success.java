package org.musigma.util;

import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;
import org.musigma.util.function.Supplier;

import java.util.NoSuchElementException;

import static org.musigma.util.Exceptions.rethrowIfFatal;

public final class Success<T> implements Try<T> {

    private final T value;

    public Success(final T value) {
        this.value = value;
    }

    @Override
    public boolean isFailure() {
        return false;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public T getOrElse(final Supplier<? extends T> defaultValue) {
        return value;
    }

    @Override
    public T getOrElse(final T defaultValue) {
        return value;
    }

    @Override
    public Try<T> orElse(final Supplier<? extends Try<T>> defaultValue) {
        return this;
    }

    @Override
    public Try<T> orElse(final Try<T> defaultValue) {
        return this;
    }

    @Override
    public <U> Try<U> map(final Function<? super T, ? extends U> function) {
        try {
            return new Success<>(function.apply(value));
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }

    @Override
    public <U> Try<U> flatMap(final Function<? super T, ? extends Try<U>> function) {
        try {
            return function.apply(value);
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }

    @Override
    public Try<T> filter(final Predicate<? super T> predicate) {
        try {
            return predicate.test(value) ? this : new Failure<>(new NoSuchElementException("predicate failed for " + value));
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }

    @Override
    public <U> Try<U> transform(final Function<? super T, ? extends U> ifSuccess, final Function<Throwable, Throwable> ifFailure) {
        try {
            return new Success<>(ifSuccess.apply(value));
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }
}
