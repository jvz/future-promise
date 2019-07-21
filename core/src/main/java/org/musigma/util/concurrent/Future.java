package org.musigma.util.concurrent;

import org.apiguardian.api.API;
import org.musigma.util.Pair;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedBiFunction;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * Declarative transformations of asynchronous computation.
 *
 * @param <T> type of value being transformed
 */
@API(status = API.Status.EXPERIMENTAL)
public interface Future<T> extends AwaitableFuture<T> {

    /**
     * Completed Future with no value.
     */
    Future<Void> VOID = fromSync(() -> null);

    /**
     * Creates a completed Future using the given success value.
     *
     * @param result success value
     * @param <T>    type of success value
     * @return a completed Future with the given success value
     */
    static <T> Future<T> successful(final T result) {
        return Promise.successful(result).future();
    }

    /**
     * Creates a completed Future using the given Exception.
     *
     * @param e   exception value
     * @param <T> type of success value
     * @return a completed Future with the given Exception
     */
    static <T> Future<T> failed(final Exception e) {
        return Promise.<T>failed(e).future();
    }

    /**
     * Creates a completed Future from the results of the given Callable using the current thread.
     *
     * @param callable provides the success value or exception
     * @param <T>      type of success value
     * @return a synchronously completed Future
     */
    static <T> Future<T> fromSync(final Callable<T> callable) {
        return Promise.from(callable).future();
    }

    /**
     * Creates an asynchronously completed Future from the results of the given Callable.
     *
     * <p><strong>Note:</strong> This uses the {@linkplain Executors#common() common pool}.</p>
     *
     * @param callable provides the success value or exception
     * @param <T>      type of success value
     * @return an asynchronously completed Future
     */
    static <T> Future<T> fromAsync(final Callable<T> callable) {
        return fromAsync(Executors.common(), callable);
    }

    /**
     * Creates an asynchronously completed Future using the given Executor from the results of the given Callable.
     *
     * @param executor where to execute the Callable to complete the Future
     * @param callable provides the success value or exception
     * @param <T>      type of success value
     * @return an asynchronously completed Future
     */
    static <T> Future<T> fromAsync(final Executor executor, final Callable<T> callable) {
        return VOID.map(executor, ignored -> callable.call());
    }

    static <T> Future<T> fromDelegate(final Callable<Future<T>> callable) {
        return fromDelegate(Executors.common(), callable);
    }

    static <T> Future<T> fromDelegate(final Executor executor, final Callable<Future<T>> callable) {
        return VOID.flatMap(executor, ignored -> callable.call());
    }

    static <T> Future<T> never() {
        return Never.getInstance();
    }

    default void onComplete(final UncheckedConsumer<Thunk<T>> consumer) {
        onComplete(Executors.common(), consumer);
    }

    void onComplete(final Executor executor, final UncheckedConsumer<Thunk<T>> consumer);

    Optional<Thunk<T>> getCurrent();

    default <U> Future<U> transform(final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function) {
        return transform(Executors.common(), function);
    }

    <U> Future<U> transform(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function);

    default <U> Future<U> transformWith(final UncheckedFunction<Thunk<T>, ? extends Future<U>> function) {
        return transformWith(Executors.common(), function);
    }

    <U> Future<U> transformWith(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Future<U>> function);

    default <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function) {
        return map(Executors.common(), function);
    }

    default <U> Future<U> map(final Executor executor, final UncheckedFunction<? super T, ? extends U> function) {
        return transform(executor, t -> t.map(function));
    }

    default <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function) {
        return flatMap(Executors.common(), function);
    }

    @SuppressWarnings("unchecked")
    default <U> Future<U> flatMap(final Executor executor, final UncheckedFunction<? super T, ? extends Future<U>> function) {
        return transformWith(executor, t -> t.isSuccess() ? function.apply(t.value()) : (Future<U>) this);
    }

    default Future<T> filter(final UncheckedPredicate<? super T> predicate) {
        return filter(Executors.common(), predicate);
    }

    default Future<T> filter(final Executor executor, final UncheckedPredicate<? super T> predicate) {
        return transform(executor, t -> t.filter(predicate));
    }

    default Future<T> recover(final UncheckedFunction<Exception, ? extends T> function) {
        return recover(Executors.common(), function);
    }

    default Future<T> recover(final Executor executor, final UncheckedFunction<Exception, ? extends T> function) {
        return transform(executor, t -> t.recover(function));
    }

    default Future<T> recoverWith(final UncheckedFunction<Exception, ? extends Future<T>> function) {
        return recoverWith(Executors.common(), function);
    }

    default Future<T> recoverWith(final Executor executor, final UncheckedFunction<Exception, ? extends Future<T>> function) {
        return transformWith(executor, t -> t.isSuccess() ? this : function.apply(t.error()));
    }

    default Future<T> fallbackTo(final Future<T> fallback) {
        return fallback == this ? this : transformWith(Executors.parasitic(), thunk -> thunk.isSuccess() ? this : fallback);
    }

    default <U> Future<Pair<T, U>> zip(final Future<U> that) {
        return zipWith(Executors.parasitic(), that, Pair::of);
    }

    default <U, R> Future<R> zipWith(final Executor executor, final Future<U> that,
                                     final UncheckedBiFunction<? super T, ? super U, ? extends R> function) {
        Executor e = executor instanceof Batching.BatchingExecutor ? executor : Executors.parasitic();
        return flatMap(e, t -> that.map(e, u -> function.apply(t, u)));
    }
}
