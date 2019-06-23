package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;
import org.musigma.util.function.UncheckedSupplier;

import java.util.Optional;

public interface Future<T> extends java.util.concurrent.Future<T> {

    Future<Void> VOID = fromThunk(Thunk.value(null));

    static <T> Future<T> successful(final T result) {
        return Promise.successful(result).future();
    }

    static <T> Future<T> failed(final Throwable throwable) {
        return Promise.<T>failed(throwable).future();
    }

    static <T> Future<T> fromThunk(final Thunk<T> thunk) {
        return Promise.fromThunk(thunk).future();
    }

    static <T> Future<T> fromAsync(final UncheckedSupplier<T> supplier) {
        return fromAsync(supplier, Scheduler.common());
    }

    static <T> Future<T> fromAsync(final UncheckedSupplier<T> supplier, final Scheduler scheduler) {
        return VOID.map(supplier, scheduler);
    }

    static <T> Future<T> fromDelegate(final UncheckedSupplier<Future<T>> supplier) {
        return fromDelegate(supplier, Scheduler.common());
    }

    static <T> Future<T> fromDelegate(final UncheckedSupplier<Future<T>> supplier, final Scheduler scheduler) {
        return VOID.flatMap(supplier, scheduler);
    }

    static <T> Future<T> never() {
        return Never.getInstance();
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
