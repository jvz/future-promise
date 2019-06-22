package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;

public interface Future<T> extends java.util.concurrent.Future<T> {

    static <T> Future<T> successful(final T result) {
        return Promise.successful(result).future();
    }

    static <T> Future<T> failed(final Throwable throwable) {
        return Promise.<T>failed(throwable).future();
    }

    default void onComplete(final UncheckedConsumer<Thunk<T>> consumer) {
        onComplete(consumer, Scheduler.common());
    }

    void onComplete(final UncheckedConsumer<Thunk<T>> consumer, final Scheduler scheduler);

    Optional<Thunk<T>> getCurrent();

    default <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function) {
        return map(function, Scheduler.common());
    }

    <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function, final Scheduler scheduler);

    default <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function) {
        return flatMap(function, Scheduler.common());
    }

    <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function, final Scheduler scheduler);

    default Future<T> filter(final UncheckedPredicate<? super T> predicate) {
        return filter(predicate, Scheduler.common());
    }

    Future<T> filter(final UncheckedPredicate<? super T> predicate, final Scheduler scheduler);

    default <U> Future<U> transform(final UncheckedFunction<Thunk<T>, Thunk<U>> function) {
        return transform(function, Scheduler.common());
    }

    <U> Future<U> transform(final UncheckedFunction<Thunk<T>, Thunk<U>> function, final Scheduler scheduler);
}
