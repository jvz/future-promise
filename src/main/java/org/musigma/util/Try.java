package org.musigma.util;

import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;
import org.musigma.util.function.Supplier;

public interface Try<T> {
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
