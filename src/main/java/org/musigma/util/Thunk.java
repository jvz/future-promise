package org.musigma.util;

import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;
import org.musigma.util.function.UncheckedSupplier;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

public final class Thunk<T> implements UncheckedSupplier<T> {

    private final Throwable error;
    private final T value;

    private Thunk(final Throwable error) {
        Exceptions.rethrowIfFatal(error);
        this.error = error;
        this.value = null;
    }

    private Thunk(final T value) {
        this.error = null;
        this.value = value;
    }

    public static <T> Thunk<T> fromSupplier(final Supplier<T> supplier) {
        try {
            return value(supplier.get());
        } catch (final Throwable throwable) {
            return error(throwable);
        }
    }

    public static <T> Thunk<T> from(final UncheckedSupplier<T> supplier) {
        try {
            return value(supplier.get());
        } catch (final Throwable throwable) {
            return error(throwable);
        }
    }

    public static <T> Thunk<T> value(final T value) {
        return new Thunk<>(value);
    }

    public static <T> Thunk<T> error(final Throwable error) {
        return new Thunk<>(error);
    }

    @Override
    public T get() throws Throwable {
        if (isError()) {
            throw error;
        }
        return value;
    }

    public boolean isError() {
        return error != null;
    }

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

    public <U> Thunk<U> flatMap(final UncheckedFunction<? super T, Thunk<U>> function) {
        if (isError()) {
            return recast();
        }
        try {
            return function.apply(value);
        } catch (final Throwable throwable) {
            return new Thunk<>(throwable);
        }
    }

    public Thunk<T> filter(final UncheckedPredicate<? super T> predicate) {
        if (isError()) {
            return this;
        }
        try {
            return predicate.test(value) ? this : new Thunk<>(new NoSuchElementException("predicated failed for " + value));
        } catch (final Throwable throwable) {
            return new Thunk<>(throwable);
        }
    }

    public <U> Thunk<U> transform(final UncheckedFunction<? super T, Thunk<U>> ifSuccess, final UncheckedFunction<Throwable, Thunk<U>> ifError) {
        try {
            return isSuccess() ? ifSuccess.apply(value) : ifError.apply(error);
        } catch (final Throwable throwable) {
            return new Thunk<>(throwable);
        }
    }
}
