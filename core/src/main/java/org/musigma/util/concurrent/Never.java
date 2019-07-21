package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class Never<T> implements Future<T> {

    private static final Never<Object> NEVER = new Never<>();

    @SuppressWarnings("unchecked")
    static <T> Never<T> getInstance() {
        return (Never<T>) NEVER;
    }

    private final CountDownLatch never = new CountDownLatch(1);

    private Never() {
    }

    @Override
    public void onComplete(final Executor executor, final UncheckedConsumer<Thunk<T>> consumer) {
    }

    @Override
    public Optional<Thunk<T>> getCurrent() {
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private <U> Future<U> recast() {
        return (Future<U>) this;
    }

    @Override
    public <U> Future<U> map(final Executor executor, final UncheckedFunction<? super T, ? extends U> function) {
        return recast();
    }

    @Override
    public <U> Future<U> flatMap(final Executor executor, final UncheckedFunction<? super T, ? extends Future<U>> function) {
        return recast();
    }

    @Override
    public Future<T> filter(final Executor executor, final UncheckedPredicate<? super T> predicate) {
        return this;
    }

    @Override
    public <U> Future<U> transform(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function) {
        return recast();
    }

    @Override
    public <U> Future<U> transformWith(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Future<U>> function) {
        return recast();
    }

    @Override
    public Future<T> recover(final Executor executor, final UncheckedFunction<Exception, ? extends T> function) {
        return this;
    }

    @Override
    public Future<T> recoverWith(final Executor executor, final UncheckedFunction<Exception, ? extends Future<T>> function) {
        return this;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        never.await();
        return null;
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        never.await(timeout, unit);
        throw new TimeoutException("future timed out after " + timeout + ' ' + unit);
    }

    @Override
    public Future<T> await() throws InterruptedException {
        never.await();
        return this;
    }

    @Override
    public Future<T> await(final long time, final TimeUnit unit) throws InterruptedException, TimeoutException {
        never.await(time, unit);
        return this;
    }
}
