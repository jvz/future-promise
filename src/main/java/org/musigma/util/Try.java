package org.musigma.util;

import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;
import org.musigma.util.function.Supplier;

public interface Try<T> {

    static <T> Try<T> fromSupplier(final Supplier<T> supplier) {
        try {
            return new Success<>(supplier.get());
        } catch (Throwable throwable) {
            return new Failure<>(throwable);
        }
    }

    static <T> Try<T> successful(final T result) {
        return new Success<>(result);
    }

    static <T> Try<T> failed(final Throwable throwable) {
        return new Failure<>(throwable);
    }

    boolean isFailure();

    boolean isSuccess();

    T getOrElse(final Supplier<? extends T> defaultValue);

    T getOrElse(final T defaultValue);

    Try<T> orElse(final Supplier<? extends Try<T>> defaultValue);

    Try<T> orElse(final Try<T> defaultValue);

    T get();

    <U> Try<U> map(final Function<? super T, ? extends U> function);

    <U> Try<U> flatMap(final Function<? super T, ? extends Try<U>> function);

    Try<T> filter(final Predicate<? super T> predicate);

    <U> Try<U> transform(final Function<? super T, ? extends U> ifSuccess, final Function<Throwable, Throwable> ifFailure);
}
