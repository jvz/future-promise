package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.musigma.util.concurrent.Transformation.Type.*;

class DefaultPromise<T> implements Promise<T>, Future<T> {

    // can be:
    // Thunk<T>
    // Callbacks<T>: Transformation<T, U> or ManyCallbacks<T>
    // Link<T>
    final AtomicReference<Object> ref;

    DefaultPromise() {
        this(Transformation.NOOP);
    }

    private DefaultPromise(final Object initialValue) {
        ref = new AtomicReference<>(initialValue);
    }

    DefaultPromise(final Callable<T> result) {
        this((Object) Thunk.from(result));
    }

    @Override
    public Optional<Thunk<T>> getCurrent() {
        return Optional.ofNullable(getCurrentValue());
    }

    @SuppressWarnings("unchecked")
    private Thunk<T> getCurrentValue() {
        final Object state = ref.get();
        if (state instanceof Thunk) {
            return (Thunk<T>) state;
        } else if (state instanceof Link) {
            return ((Link<T>) state).promise(this).getCurrentValue();
        } else {
            // state instanceof Callbacks
            return null;
        }
    }

    @Override
    public Future<T> future() {
        return this;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        final Thunk<T> thunk = tryGet();
        try {
            return thunk.call();
        } catch (final Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void await() throws InterruptedException {
        tryGet();
    }

    private Thunk<T> tryGet() throws InterruptedException {
        final Thunk<T> v = getCurrentValue();
        return v != null ? v : awaitResult();
    }

    private Thunk<T> awaitResult() throws InterruptedException {
        final CompletionLatch<T> latch = new CompletionLatch<>();
        onComplete(latch, Scheduler.parasitic());
        latch.acquireSharedInterruptibly(1);
        return latch.getResult();
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final Thunk<T> thunk = tryGet(timeout, unit);
        try {
            return thunk.call();
        } catch (final Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void await(final long time, final TimeUnit unit) throws InterruptedException, TimeoutException {
        final Thunk<T> result = tryGet(time, unit);
        if (result == null) {
            throw new TimeoutException("future timed out after " + time + " " + unit);
        }
    }

    private Thunk<T> tryGet(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        final Thunk<T> v = getCurrentValue();
        if (v != null) {
            return v;
        }
        final Thunk<T> result = timeout < 0 ? null : timeout == 0 ? awaitResult() : awaitResult(timeout, unit);
        if (result == null) {
            throw new TimeoutException("future timed out after " + timeout + " " + unit);
        }
        return result;
    }

    private Thunk<T> awaitResult(final long timeout, final TimeUnit unit) throws InterruptedException {
        final CompletionLatch<T> latch = new CompletionLatch<>();
        onComplete(latch, Scheduler.parasitic());
        latch.tryAcquireSharedNanos(1, unit.toNanos(timeout));
        return latch.getResult();
    }

    @Override
    public boolean isDone() {
        return getCurrentValue() != null;
    }

    @Override
    public boolean tryComplete(final Callable<T> result) {
        final Object state = ref.get();
        return !(state instanceof Thunk) && tryComplete(state, result);
    }

    @SuppressWarnings("unchecked")
    boolean tryComplete(final Object state, final Callable<T> resolved) {
        Object currentState = state;
        while (true) {
            if (currentState instanceof Callbacks) {
                final Callbacks<T> callbacks = (Callbacks<T>) currentState;
                if (ref.compareAndSet(callbacks, resolved)) {
                    if (callbacks != Transformation.NOOP) {
                        callbacks.submitWithValue(resolved);
                    }
                    return true;
                } else {
                    currentState = ref.get();
                }
            } else if (currentState instanceof Link) {
                final DefaultPromise<T> promise = ((Link<T>) currentState).promise(this);
                return promise != this && promise.tryComplete(promise.ref.get(), resolved);
            } else {
                return false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void linkRootOf(final DefaultPromise<T> target, final Link<T> link) {
        if (target == this) {
            return;
        }
        for (Object state = ref.get(); ; state = ref.get()) {
            if (state instanceof Callable) {
                final Callable<T> value = (Callable<T>) state;
                if (!target.tryComplete(target.ref.get(), value)) {
                    throw new IllegalStateException("cannot link promises");
                }
                break;
            } else if (state instanceof Callbacks) {
                final Callbacks<T> callbacks = (Callbacks<T>) state;
                final Link<T> l = link != null ? link : new Link<>(target);
                final DefaultPromise<T> promise = l.promise(this);
                if (promise != this && ref.compareAndSet(callbacks, l)) {
                    if (callbacks != Transformation.NOOP) {
                        promise.dispatchOrAddCallbacks(callbacks);
                    }
                    break;
                } // else retry
            } else {
                ((Link<T>) state).promise(this).linkRootOf(target, link);
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void unlink(final Callable<T> resolved) {
        final Object state = ref.get();
        if (state instanceof Link) {
            final Link<T> link = (Link<T>) state;
            final DefaultPromise<T> next = ref.compareAndSet(link, resolved) ? link.ref.get() : this;
            next.unlink(resolved);
        } else {
            tryComplete(state, resolved);
        }
    }

    @SuppressWarnings("unchecked")
    private <C extends Callbacks<T>> C dispatchOrAddCallbacks(final C callbacks) {
        for (Object state = ref.get(); ; state = ref.get()) {
            if (state instanceof Callable) {
                callbacks.submitWithValue((Callable<T>) state);
                return callbacks;
            } else if (state instanceof Callbacks) {
                final Callbacks<T> value = (Callbacks<T>) state;
                if (ref.compareAndSet(value, value == Transformation.NOOP ? callbacks : callbacks.concat(value))) {
                    return callbacks;
                } // else retry
            } else {
                final DefaultPromise<T> promise = ((Link<T>) state).promise(this);
                return promise.dispatchOrAddCallbacks(callbacks);
            }
        }
    }

    @Override
    public void onComplete(final UncheckedConsumer<Thunk<T>> consumer, final Scheduler scheduler) {
        dispatchOrAddCallbacks(onComplete.using(consumer, scheduler));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return (Future<U>) this;
        } else {
            return dispatchOrAddCallbacks(map.using(function, scheduler));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Future<U> flatMap(final UncheckedFunction<? super T, ? extends Future<U>> function, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return (Future<U>) this;
        } else {
            return dispatchOrAddCallbacks(flatMap.using(function, scheduler));
        }
    }

    @Override
    public Future<T> filter(final UncheckedPredicate<? super T> predicate, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return this;
        } else {
            return dispatchOrAddCallbacks(filter.using(predicate, scheduler));
        }
    }

    @Override
    public <U> Future<U> transform(final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function, final Scheduler scheduler) {
        return dispatchOrAddCallbacks(transform.using(function, scheduler));
    }

    @Override
    public <U> Future<U> transformWith(final UncheckedFunction<Thunk<T>, ? extends Future<T>> function, final Scheduler scheduler) {
        return dispatchOrAddCallbacks(transformWith.using(function, scheduler));
    }

    @Override
    public Future<T> recover(final UncheckedFunction<Exception, ? extends T> function, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isSuccess()) {
            // recover fast
            return this;
        } else {
            return dispatchOrAddCallbacks(recover.using(function, scheduler));
        }
    }

    @Override
    public Future<T> recoverWith(final UncheckedFunction<Exception, ? extends Future<T>> function, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isSuccess()) {
            // recover fast
            return this;
        } else {
            return dispatchOrAddCallbacks(recoverWith.using(function, scheduler));
        }
    }
}
