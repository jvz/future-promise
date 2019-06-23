package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
    public void onComplete(final UncheckedConsumer<Thunk<T>> consumer, final Scheduler scheduler) {
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
    public <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function, final Scheduler scheduler) {
        return recast();
    }

    @Override
    public <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function, final Scheduler scheduler) {
        return recast();
    }

    @Override
    public Future<T> filter(final UncheckedPredicate<? super T> predicate, final Scheduler scheduler) {
        return this;
    }

    @Override
    public <U> Future<U> transform(final UncheckedFunction<Thunk<T>, Thunk<U>> function, final Scheduler scheduler) {
        return recast();
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
}
