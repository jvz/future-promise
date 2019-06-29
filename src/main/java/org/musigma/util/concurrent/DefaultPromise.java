package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
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

class DefaultPromise<T> implements Promise<T>, Future<T> {

    // can be:
    // Callable<T> or Thunk<T>
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
        this((Object) result);
    }

    @Override
    public Optional<Callable<T>> getCurrent() {
        return Optional.ofNullable(getCurrentValue());
    }

    @SuppressWarnings("unchecked")
    private Callable<T> getCurrentValue() {
        final Object state = ref.get();
        if (state instanceof Callable) {
            return (Callable<T>) state;
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
        try {
            return get(0, TimeUnit.SECONDS);
        } catch (final TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return tryGet(timeout, unit).call();
        } catch (final Exception throwable) {
            Exceptions.rethrowUnchecked(throwable);
            return null;
        }
    }

    private Callable<T> tryGet(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        final Callable<T> v = getCurrentValue();
        if (v != null) {
            return v;
        }
        Callable<T> result = null;
        if (timeout >= 0) {
            final CompletionLatch<T> latch = new CompletionLatch<>();
            onComplete(latch, Scheduler.parasitic());
            if (timeout == 0) {
                latch.acquireSharedInterruptibly(1);
            } else {
                latch.tryAcquireSharedNanos(1, unit.toNanos(timeout));
            }
            result = latch.getResult();
        }
        if (result == null) {
            throw new TimeoutException("future timed out after " + timeout + " " + unit);
        }
        return result;
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
            } else if (state instanceof Link) {
                final DefaultPromise<T> promise = ((Link<T>) state).promise(this);
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
        dispatchOrAddCallbacks(new Transformation<T, Void>(consumer, scheduler, null, Transform.onComplete));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Future<U> map(final UncheckedFunction<? super T, ? extends U> function, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return (Future<U>) this;
        } else {
            return dispatchOrAddCallbacks(new Transformation<>(function, scheduler, null, Transform.map));
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
            return dispatchOrAddCallbacks(new Transformation<>(function, scheduler, null, Transform.flatMap));
        }
    }

    @Override
    public Future<T> filter(final UncheckedPredicate<? super T> predicate, final Scheduler scheduler) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return this;
        } else {
            return dispatchOrAddCallbacks(new Transformation<>(predicate, scheduler, null, Transform.filter));
        }
    }

    @Override
    public <U> Future<U> transform(final UncheckedFunction<Thunk<T>, Thunk<U>> function, final Scheduler scheduler) {
        return dispatchOrAddCallbacks(new Transformation<>(function, scheduler, null, Transform.transform));
    }

    @Override
    public <U> Future<U> transformWith(final UncheckedFunction<Thunk<T>, Future<T>> function, final Scheduler scheduler) {
        return dispatchOrAddCallbacks(new Transformation<>(function, scheduler, null, Transform.transformWith));
    }
}
