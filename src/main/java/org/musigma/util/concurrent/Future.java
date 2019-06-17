package org.musigma.util.concurrent;

import org.musigma.util.Try;
import org.musigma.util.function.Consumer;
import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;

import java.util.Optional;

public interface Future<T> extends java.util.concurrent.Future<T> {

    static <T> Future<T> fromTry(final Try<T> result) {
        return Promise.fromTry(result).future();
    }

    static <T> Future<T> successful(final T result) {
        return Promise.successful(result).future();
    }

    static <T> Future<T> failed(final Throwable throwable) {
        return Promise.<T>failed(throwable).future();
    }

    default void onComplete(final Consumer<Try<T>> consumer) {
        onComplete(consumer, Scheduler.common());
    }

    void onComplete(final Consumer<Try<T>> consumer, final Scheduler scheduler);

    Optional<Try<T>> getCurrent();

    default <U> Future<U> map(final Function<? super T, ? extends U> function) {
        return map(function, Scheduler.common());
    }

    <U> Future<U> map(final Function<? super T, ? extends U> function, final Scheduler scheduler);

    default <U> Future<U> flatMap(final Function<? super T, ? extends Future<U>> function) {
        return flatMap(function, Scheduler.common());
    }

    <U> Future<U> flatMap(final Function<? super T, ? extends Future<U>> function, final Scheduler scheduler);

    default Future<T> filter(final Predicate<? super T> predicate) {
        return filter(predicate, Scheduler.common());
    }

    Future<T> filter(final Predicate<? super T> predicate, final Scheduler scheduler);

    default <U> Future<U> transform(final Function<Try<T>, Try<U>> function) {
        return transform(function, Scheduler.common());
    }

    <U> Future<U> transform(final Function<Try<T>, Try<U>> function, final Scheduler scheduler);
}
