package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import static org.musigma.util.concurrent.DefaultPromise.Transformation.Type.*;

class DefaultPromise<T> implements Promise<T>, Future<T> {

    // can be:
    // Thunk<T>
    // Callbacks<T>: Transformation<T, U> or ManyCallbacks<T>
    // Link<T>
    final AtomicReference<Object> ref;

    // TODO: extract a promise factory SPI of some sort

    /**
     * Constructs an unfulfilled promise.
     */
    DefaultPromise() {
        this(Transformation.NOOP);
    }

    /**
     * Constructs a pre-filled promise from the result of a Callable.
     */
    DefaultPromise(final Callable<T> result) {
        this((Object) Thunk.from(result));
    }

    private DefaultPromise(final Object initialValue) {
        ref = new AtomicReference<>(initialValue);
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
        onComplete(Executors.parasitic(), latch);
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
        onComplete(Executors.parasitic(), latch);
        latch.tryAcquireSharedNanos(1, unit.toNanos(timeout));
        return latch.getResult();
    }

    @Override
    public boolean isDone() {
        return getCurrentValue() != null;
    }

    @Override
    public boolean tryComplete(final Callable<T> callable) {
        final Object state = ref.get();
        return !(state instanceof Thunk) && tryComplete(state, callable);
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
    public void onComplete(final Executor executor, final UncheckedConsumer<Thunk<T>> consumer) {
        dispatchOrAddCallbacks(onComplete.using(consumer, executor));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Future<U> map(final Executor executor, final UncheckedFunction<? super T, ? extends U> function) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return (Future<U>) this;
        } else {
            return dispatchOrAddCallbacks(map.using(function, executor));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Future<U> flatMap(final Executor executor, final UncheckedFunction<? super T, ? extends Future<U>> function) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return (Future<U>) this;
        } else {
            return dispatchOrAddCallbacks(flatMap.using(function, executor));
        }
    }

    @Override
    public Future<T> filter(final Executor executor, final UncheckedPredicate<? super T> predicate) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isError()) {
            // fail fast
            return this;
        } else {
            return dispatchOrAddCallbacks(filter.using(predicate, executor));
        }
    }

    @Override
    public <U> Future<U> transform(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Callable<U>> function) {
        return dispatchOrAddCallbacks(transform.using(function, executor));
    }

    @Override
    public <U> Future<U> transformWith(final Executor executor, final UncheckedFunction<Thunk<T>, ? extends Future<U>> function) {
        return dispatchOrAddCallbacks(transformWith.using(function, executor));
    }

    @Override
    public Future<T> recover(final Executor executor, final UncheckedFunction<Exception, ? extends T> function) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isSuccess()) {
            // recover fast
            return this;
        } else {
            return dispatchOrAddCallbacks(recover.using(function, executor));
        }
    }

    @Override
    public Future<T> recoverWith(final Executor executor, final UncheckedFunction<Exception, ? extends Future<T>> function) {
        final Object state = ref.get();
        if (state instanceof Thunk && ((Thunk<?>) state).isSuccess()) {
            // recover fast
            return this;
        } else {
            return dispatchOrAddCallbacks(recoverWith.using(function, executor));
        }
    }

    private interface Callbacks<T> {
        Callbacks<T> concat(final Callbacks<T> next);

        void submitWithValue(final Callable<T> resolved);
    }

    private static class CompletionLatch<T> extends AbstractQueuedSynchronizer implements UncheckedConsumer<Thunk<T>> {

        // avoid using volatile by using acquire/release
        private Thunk<T> result;

        Thunk<T> getResult() {
            return result;
        }

        @Override
        protected int tryAcquireShared(final int arg) {
            return getState() != 0 ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(final int arg) {
            setState(1);
            return true;
        }

        @Override
        public void accept(final Thunk<T> value) {
            result = value;
            releaseShared(1);
        }

    }

    private static class Link<T> {

        final AtomicReference<DefaultPromise<T>> ref;

        Link(final DefaultPromise<T> to) {
            ref = new AtomicReference<>(to);
        }

        @SuppressWarnings("unchecked")
        DefaultPromise<T> promise(final DefaultPromise<T> owner) {
            DefaultPromise<T> current = ref.get();
            DefaultPromise<T> target = current;
            while (true) {
                final Object value = target.ref.get();
                if (value instanceof DefaultPromise.Callbacks) {
                    if (ref.compareAndSet(current, target)) {
                        return target; // linked
                    } else {
                        current = ref.get();
                    }
                } else if (value instanceof DefaultPromise.Link) {
                    target = ((Link<T>) value).ref.get();
                } else {
                    owner.unlink((Callable<T>) value);
                    return owner;
                }
            }
        }

    }

    private static class ManyCallbacks<T> implements Callbacks<T> {

        private final Transformation<T, ?> head;
        private final Callbacks<T> tail;

        ManyCallbacks(final Transformation<T, ?> head, final Callbacks<T> tail) {
            this.head = head;
            this.tail = tail;
        }

        @Override
        public Callbacks<T> concat(final Callbacks<T> next) {
            return tail.concat(new ManyCallbacks<>(head, next));
        }

        @Override
        public void submitWithValue(final Callable<T> resolved) {
            head.submitWithValue(resolved);
            tail.submitWithValue(resolved);
        }
    }

    @Batchable
    static class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {

        enum Type {
            noop, map, flatMap, transform, transformWith, onComplete, recover, recoverWith, filter;

            <F, T> Transformation<F, T> using(final UncheckedFunction<?, ?> function, final Executor executor) {
                return new Transformation<>(function, executor, null, this);
            }
        }

        static Transformation<?, ?> NOOP = noop.using(UncheckedFunction.identity(), Executors.parasitic());

        private UncheckedFunction<Object, Object> function;
        private Executor executor;
        private Callable<F> argument;
        private Type transformType;

        @SuppressWarnings("unchecked")
        private Transformation(final UncheckedFunction<?, ?> function, final Executor executor, final Callable<F> argument, final Type transformType) {
            super();
            this.function = (UncheckedFunction<Object, Object>) function;
            this.executor = executor;
            this.argument = argument;
            this.transformType = transformType;
        }

        @Override
        public void submitWithValue(final Callable<F> resolved) {
            argument = resolved;
            try {
                executor.execute(this);
            } catch (final Exception e) {
                final Executor executor = this.executor;
                this.executor = null;
                function = null;
                argument = null;
                handleFailure(e, executor);
            }
        }

        private void handleFailure(final Exception e, final Executor executor) {
            final boolean interrupted = e instanceof InterruptedException;
            final boolean completed = tryComplete(ref.get(), Thunk.error(e));
            if (completed && interrupted) {
                Thread.currentThread().interrupt();
            }
            if (transformType == onComplete || !completed) {
                if (executor instanceof Batching.BatchingExecutor) {
                    ((Batching.BatchingExecutor) executor).exceptionHandler.accept(e);
                } else {
                    e.printStackTrace();
                }
            }
        }

        boolean benefitsFromBatching() {
            return transformType != onComplete;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            final Thunk<F> value = Thunk.from(this.argument);
            this.argument = null;
            final UncheckedFunction<Object, Object> function = this.function;
            this.function = null;
            final Executor executor = this.executor;
            this.executor = null;
            try {
                Callable<T> resolvedResult = null;
                switch (transformType) {
                    case noop:
                        break;

                    case map: {
                        final UncheckedFunction<? super F, ? extends T> f = (UncheckedFunction<? super F, ? extends T>) function;
                        resolvedResult = value.map(f);
                        break;
                    }

                    case flatMap: {
                        if (value.isSuccess()) {
                            final UncheckedFunction<? super F, ? extends Future<T>> f = (UncheckedFunction) function;
                            final Future<T> future = f.apply(value.call());
                            if (future instanceof DefaultPromise) {
                                ((DefaultPromise<T>) future).linkRootOf(this, null);
                            } else {
                                completeWith(future);
                            }
                        } else {
                            resolvedResult = value.recastIfError();
                        }
                        break;
                    }

                    case transform: {
                        final UncheckedFunction<Thunk<F>, ? extends Callable<T>> f = (UncheckedFunction) function;
                        resolvedResult = f.apply(value);
                        break;
                    }

                    case transformWith: {
                        final UncheckedFunction<Thunk<F>, ? extends Future<T>> f = (UncheckedFunction) function;
                        final Future<T> future = f.apply(value);
                        if (future instanceof DefaultPromise) {
                            ((DefaultPromise<T>) future).linkRootOf(this, null);
                        } else {
                            completeWith(future);
                        }
                        break;
                    }

                    case onComplete: {
                        final UncheckedConsumer<Thunk<F>> onComplete = (UncheckedConsumer) function;
                        onComplete.accept(value);
                        break;
                    }

                    case recover: {
                        // F == T
                        final Thunk<F> fallback;
                        if (value.isSuccess()) {
                            fallback = value;
                        } else {
                            final UncheckedFunction<Exception, ? extends F> f = (UncheckedFunction) function;
                            fallback = value.recover(f);
                        }
                        resolvedResult = fallback.recast();
                        break;
                    }

                    case recoverWith: {
                        // F == T
                        if (value.isSuccess()) {
                            resolvedResult = value.recast();
                        } else {
                            final UncheckedFunction<Exception, ? extends Future<T>> f = (UncheckedFunction) function;
                            final Future<T> fallback = f.apply(value.error());
                            if (fallback instanceof DefaultPromise) {
                                ((DefaultPromise<T>) fallback).linkRootOf(this, null);
                            } else {
                                completeWith(fallback);
                            }
                            resolvedResult = null;
                        }
                        break;
                    }

                    case filter: {
                        // F == T
                        final UncheckedPredicate<? super F> predicate = (UncheckedPredicate) function;
                        resolvedResult = value.filter(predicate).recast();
                        break;
                    }

                    default:
                        throw new UnsupportedOperationException("Unknown transformation type");
                }
                if (resolvedResult != null) {
                    tryComplete(ref.get(), resolvedResult);
                }
            } catch (Exception e) {
                handleFailure(e, executor);
            }
        }

        @Override
        public Callbacks<F> concat(final Callbacks<F> next) {
            return new ManyCallbacks<>(this, next);
        }

    }
}
