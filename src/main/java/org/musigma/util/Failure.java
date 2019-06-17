package org.musigma.util;

import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;
import org.musigma.util.function.Supplier;

import static org.musigma.util.Exceptions.rethrow;
import static org.musigma.util.Exceptions.rethrowIfFatal;

public final class Failure<T> implements Try<T> {

    private final Throwable throwable;

    public Failure(final Throwable throwable) {
        this.throwable = throwable;
    }

    @Override
    public boolean isFailure() {
        return true;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public T get() {
        rethrow(throwable);
        return null;
    }

    @Override
    public T getOrElse(final Supplier<? extends T> defaultValue) {
        try {
            return defaultValue.get();
        } catch (final Throwable t) {
            rethrow(t);
            return null;
        }
    }

    @Override
    public T getOrElse(final T defaultValue) {
        return defaultValue;
    }

    @Override
    public Try<T> orElse(final Supplier<? extends Try<T>> defaultValue) {
        try {
            return defaultValue.get();
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }

    @Override
    public Try<T> orElse(final Try<T> defaultValue) {
        return defaultValue;
    }

    @Override
    public <U> Try<U> map(final Function<? super T, ? extends U> function) {
        return new Failure<>(throwable);
    }

    @Override
    public <U> Try<U> flatMap(final Function<? super T, ? extends Try<U>> function) {
        return new Failure<>(throwable);
    }

    @Override
    public Try<T> filter(final Predicate<? super T> predicate) {
        return this;
    }

    @Override
    public <U> Try<U> transform(final Function<? super T, ? extends U> ifSuccess, final Function<Throwable, Throwable> ifFailure) {
        try {
            return new Failure<>(ifFailure.apply(throwable));
        } catch (final Throwable t) {
            rethrowIfFatal(t);
            return new Failure<>(t);
        }
    }
}
