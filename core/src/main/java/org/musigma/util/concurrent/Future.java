package org.musigma.util.concurrent;

import org.musigma.util.Pair;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedBiFunction;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

// TODO: a lot of these methods can provide default implementations
public interface Future<T> extends AwaitableFuture<T> {

    Future<Void> VOID = from(() -> null);

    static <T> Future<T> successful(final T result) {
        return Promise.successful(result).future();
    }

    static <T> Future<T> failed(final Exception e) {
        return Promise.<T>failed(e).future();
    }

    static <T> Future<T> from(final Callable<T> thunk) {
        return Promise.from(thunk).future();
    }

    static <T> Future<T> fromAsync(final Callable<T> callable) {
        return fromAsync(callable, Executors.common());
    }

    static <T> Future<T> fromAsync(final Callable<T> callable, final ExecutorService scheduler) {
        return VOID.map(ignored -> callable.call(), scheduler);
    }

    static <T> Future<T> fromDelegate(final Callable<Future<T>> callable) {
        return fromDelegate(callable, Executors.common());
    }

    static <T> Future<T> fromDelegate(final Callable<Future<T>> callable, final ExecutorService scheduler) {
        return VOID.flatMap(ignored -> callable.call(), scheduler);
    }

    static <T> Future<T> never() {
        return Never.getInstance();
    }

    default void onComplete(final UncheckedConsumer<Thunk<T>> consumer) {
        onComplete(consumer, Executors.common());
    }

    void onComplete(final UncheckedConsumer<Thunk<T>> consumer, final ExecutorService scheduler);

    Optional<Thunk<T>> getCurrent();

    default <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function) {
        return map(function, Executors.common());
    }

    <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function, final ExecutorService scheduler);

    default <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function) {
        return flatMap(function, Executors.common());
    }

    <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function, final ExecutorService scheduler);

    default Future<T> filter(final UncheckedPredicate<? super T> predicate) {
        return filter(predicate, Executors.common());
    }

    Future<T> filter(final UncheckedPredicate<? super T> predicate, final ExecutorService scheduler);

    default <U> Future<U> transform(final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function) {
        return transform(function, Executors.common());
    }

    <U> Future<U> transform(final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function, final ExecutorService scheduler);

    default <U> Future<U> transformWith(final UncheckedFunction<Thunk<T>, ? extends Future<T>> function) {
        return transformWith(function, Executors.common());
    }

    <U> Future<U> transformWith(final UncheckedFunction<Thunk<T>, ? extends Future<T>> function, final ExecutorService scheduler);

    default Future<T> recover(final UncheckedFunction<Exception, ? extends T> function) {
        return recover(function, Executors.common());
    }

    Future<T> recover(final UncheckedFunction<Exception, ? extends T> function, final ExecutorService scheduler);

    default Future<T> recoverWith(final UncheckedFunction<Exception, ? extends Future<T>> function) {
        return recoverWith(function, Executors.common());
    }

    Future<T> recoverWith(final UncheckedFunction<Exception, ? extends Future<T>> function, final ExecutorService scheduler);

    default Future<T> fallbackTo(final Future<T> fallback) {
        return fallback == this ? this : transformWith(thunk -> thunk.isSuccess() ? this : fallback, Executors.parasitic());
    }

    default <U> Future<Pair<T, U>> zip(final Future<U> that) {
        return zipWith(that, Pair::of, Executors.parasitic());
    }

    default <U, R> Future<R> zipWith(final Future<U> that,
                                     final UncheckedBiFunction<? super T, ? super U, ? extends R> function,
                                     final ExecutorService scheduler) {
        ExecutorService s = scheduler instanceof Batching.BatchingExecutorService ? scheduler : Executors.parasitic();
        return flatMap(t -> that.map(u -> function.apply(t, u), s), s);
    }
}
