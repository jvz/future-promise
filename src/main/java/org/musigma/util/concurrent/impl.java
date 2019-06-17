package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.Failure;
import org.musigma.util.Success;
import org.musigma.util.Try;
import org.musigma.util.function.Consumer;
import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

final class impl {

    private static class CompletionLatch<T> extends AbstractQueuedSynchronizer implements Consumer<Try<T>> {
        // avoid using volatile by using acquire/release
        private Try<T> result;

        private Try<T> getResult() {
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
        public void accept(final Try<T> value) throws Throwable {
            result = value;
            releaseShared(1);
        }

    }

    private static class Link<T> {
        private final AtomicReference<DefaultPromise<T>> ref;

        private Link(final DefaultPromise<T> to) {
            ref = new AtomicReference<>(to);
        }

        private DefaultPromise<T> promise(final DefaultPromise<T> owner) {
            DefaultPromise<T> current = ref.get();
            return compressed(current, current, owner);
        }

        @SuppressWarnings("unchecked")
        private DefaultPromise<T> compressed(final DefaultPromise<T> current, final DefaultPromise<T> target, final DefaultPromise<T> owner) {
            Object value = target.ref.get();
            if (value instanceof Callbacks) {
                if (ref.compareAndSet(current, target)) {
                    return target; // linked
                } else {
                    return compressed(ref.get(), target, owner); // retry
                }
            } else if (value instanceof Link) {
                return compressed(current, ((Link<T>) value).ref.get(), owner);
            } else {
                final Try<T> t = (Try<T>) value;
                owner.unlink(t);
                return owner;
            }
        }
    }

    private static <T> Try<T> resolve(final Try<T> value) {
        Objects.requireNonNull(value);
        // TODO: this can support the use of a ControlThrowable and similar
        return value;
    }

    static class DefaultPromise<T> implements Promise<T>, Future<T> {

        final AtomicReference<Object> ref;

        DefaultPromise(final Object initialValue) {
            ref = new AtomicReference<>(initialValue);
        }

        @Override
        public Optional<Try<T>> getCurrent() {
            return Optional.ofNullable(getCurrentValue());
        }

        @SuppressWarnings("unchecked")
        private Try<T> getCurrentValue() {
            final Object state = ref.get();
            if (state instanceof Try) {
                return (Try<T>) state;
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
            return tryGet(timeout, unit).get();
        }

        private Try<T> tryGet(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
            final Try<T> v = getCurrentValue();
            if (v != null) {
                return v;
            }
            Try<T> result = null;
            if (timeout >= 0) {
                final CompletionLatch<T> latch = new CompletionLatch<>();
                onComplete(latch, Scheduler.inline());
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
        public boolean tryComplete(final Try<T> result) {
            final Object state = ref.get();
            return !(state instanceof Try) && tryComplete(state, result);
        }

        @SuppressWarnings("unchecked")
        boolean tryComplete(final Object state, final Try<T> resolved) {
            Object currentState = state;
            while (true) {
                if (currentState instanceof Callbacks) {
                    final Callbacks<T> callbacks = (Callbacks<T>) currentState;
                    if (ref.compareAndSet(callbacks, resolved)) {
                        if (callbacks != NOOP) {
                            submitWithValue(callbacks, resolved);
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

        private void submitWithValue(final Callbacks<T> callbacks, final Try<T> resolved) {
            Callbacks<T> c = callbacks;
            while (c instanceof ManyCallbacks) {
                final ManyCallbacks<T> m = (ManyCallbacks<T>) c;
                m.head.submitWithValue(resolved);
                c = m.tail;
            }
            if (callbacks instanceof Transformation) {
                ((Transformation<T, ?>) callbacks).submitWithValue(resolved);
            }
        }

        @SuppressWarnings("unchecked")
        void linkRootOf(final DefaultPromise<T> target, final Link<T> link) {
            if (target == this) {
                return;
            }
            final Object state = ref.get();
            if (state instanceof Try) {
                Try<T> value = (Try<T>) state;
                if (!target.tryComplete(target.ref.get(), value)) {
                    throw new IllegalStateException("cannot link promises");
                }
            } else if (state instanceof Callbacks) {
                final Callbacks<T> callbacks = (Callbacks<T>) state;
                final Link<T> l = link != null ? link : new Link<>(target);
                final DefaultPromise<T> promise = l.promise(this);
                if (promise != this && ref.compareAndSet(callbacks, l)) {
                    if (callbacks != NOOP) {
                        promise.dispatchOrAddCallbacks(promise.ref.get(), callbacks);
                    }
                } else {
                    // FIXME: unroll
                    linkRootOf(promise, l);
                }
            } else {
                ((Link<T>) state).promise(this).linkRootOf(target, link);
            }
        }

        @SuppressWarnings("unchecked")
        void unlink(final Try<T> resolved) {
            final Object state = ref.get();
            if (state instanceof Link) {
                Link<T> link = (Link<T>) state;
                final DefaultPromise<T> next = ref.compareAndSet(link, resolved) ? link.ref.get() : this;
                next.unlink(resolved);
            } else {
                tryComplete(state, resolved);
            }
        }

        @SuppressWarnings("unchecked")
        private <C extends Callbacks<T>> C dispatchOrAddCallbacks(final Object state, final C callbacks) {
            if (state instanceof Try) {
                final Try<T> value = (Try<T>) state;
                submitWithValue(callbacks, value);
                return callbacks;
            } else if (state instanceof Callbacks) {
                final Callbacks value = (Callbacks) state;
                if (ref.compareAndSet(value, value == NOOP ? callbacks : concatCallbacks(callbacks, value))) {
                    return callbacks;
                } else {
                    // FIXME: unroll
                    return dispatchOrAddCallbacks(ref.get(), callbacks);
                }
            } else {
                DefaultPromise<T> promise = ((Link<T>) state).promise(this);
                return promise.dispatchOrAddCallbacks(promise.ref.get(), callbacks);
            }
        }

        private Callbacks<T> concatCallbacks(final Callbacks<T> left, final Callbacks<T> right) {
            if (left instanceof Transformation) {
                final Transformation<T, ?> transformation = (Transformation<T, ?>) left;
                return new ManyCallbacks<>(transformation, right);
            } else {
                final ManyCallbacks<T> m = (ManyCallbacks<T>) left;
                // FIXME: unroll
                return concatCallbacks(m.tail, new ManyCallbacks<>(m.head, right));
            }
        }

        @Override
        public void onComplete(final Consumer<Try<T>> consumer, final Scheduler scheduler) {
            dispatchOrAddCallbacks(ref.get(), new Transformation<T, Void>(consumer, scheduler, null, Transform.onComplete));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <U> Future<U> map(final Function<? super T, ? extends U> function, final Scheduler scheduler) {
            final Object state = ref.get();
            if (state instanceof Failure) {
                return (Future<U>) this;
            } else {
                return dispatchOrAddCallbacks(state, new Transformation<>(function, scheduler, null, Transform.map));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <U> Future<U> flatMap(final Function<? super T, ? extends Future<U>> function, final Scheduler scheduler) {
            final Object state = ref.get();
            if (state instanceof Failure) {
                return (Future<U>) this;
            } else {
                return dispatchOrAddCallbacks(state, new Transformation<>(function, scheduler, null, Transform.flatMap));
            }
        }

        @Override
        public Future<T> filter(final Predicate<? super T> predicate, final Scheduler scheduler) {
            final Object state = ref.get();
            if (state instanceof Failure) {
                return this;
            } else {
                return dispatchOrAddCallbacks(state, new Transformation<>(predicate, scheduler, null, Transform.filter));
            }
        }

        @Override
        public <U> Future<U> transform(final Function<Try<T>, Try<U>> function, final Scheduler scheduler) {
            return dispatchOrAddCallbacks(ref.get(), new Transformation<>(function, scheduler, null, Transform.transform));
        }
    }

    private interface Callbacks<T> {
    }

    private static class ManyCallbacks<T> implements Callbacks<T> {
        private final Transformation<T, ?> head;
        private final Callbacks<T> tail;

        private ManyCallbacks(final Transformation<T, ?> head, final Callbacks<T> tail) {
            this.head = head;
            this.tail = tail;
        }
    }

    private static Transformation<?, ?> NOOP = new Transformation<>(null, Scheduler.inline(), null, Transform.noop);

    private enum Transform {
        noop, map, flatMap, transform, onComplete, filter
    }


    private static class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {
        private Function<Object, Object> function;
        private Scheduler scheduler;
        private Try<F> argument;
        private Transform transform;

        @SuppressWarnings("unchecked")
        private Transformation(final Function<?, ?> function, final Scheduler scheduler, final Try<F> argument, final Transform transform) {
            super(NOOP);
            this.function = (Function<Object, Object>) function;
            this.scheduler = scheduler;
            this.argument = argument;
            this.transform = transform;
        }

        private void submitWithValue(final Try<F> resolved) {
            argument = resolved;
            try {
                scheduler.execute(this);
            } catch (final Throwable t) {
                Scheduler scheduler = this.scheduler;
                this.scheduler = null;
                function = null;
                argument = null;
                handleFailure(t, scheduler);
            }
        }

        private void handleFailure(final Throwable t, final Scheduler scheduler) {
            boolean wasInterrupted = t instanceof InterruptedException;
            if (!wasInterrupted) {
                Exceptions.rethrowIfFatal(t);
            }
            boolean completed = tryComplete(ref.get(), resolve(new Failure<>(t)));
            if (completed && wasInterrupted) {
                Thread.currentThread().interrupt();
            }
            if (transform == Transform.onComplete || !completed) {
                scheduler.handleException(t);
            } else {
                Exceptions.rethrow(t);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            final Try<F> value = this.argument;
            this.argument = null;
            final Function<Object, Object> function = this.function;
            this.function = null;
            final Scheduler scheduler = this.scheduler;
            this.scheduler = null;
            try {
                Try<?> resolvedResult = null;
                switch (transform) {
                    case noop:
                        break;

                    case map:
                        resolvedResult = value.isSuccess() ? new Success<>(function.apply(value.get())) : value;
                        break;

                    case flatMap:
                        if (value.isSuccess()) {
                            final Object f = function.apply(value.get());
                            if (f instanceof DefaultPromise) {
                                ((DefaultPromise<T>) f).linkRootOf(this, null);
                            } else {
                                completeWith((Future<T>) f);
                            }
                        } else {
                            resolvedResult = value;
                        }
                        break;

                    case transform:
                        resolvedResult = resolve((Try<T>) function.apply(value));
                        break;

                    case onComplete:
                        function.apply(value);
                        break;

                    case filter:
                        final Predicate<F> predicate = (Predicate) function;
                        resolvedResult = value.isFailure() || predicate.test(value.get()) ? value : new Failure<>(new NoSuchElementException("filter predicate failed"));
                        break;

                    default:
                        throw new UnsupportedOperationException("Unknown transformation type");
                }
                if (resolvedResult != null) {
                    tryComplete(ref.get(), (Try<T>) resolvedResult);
                }
            } catch (Throwable throwable) {
                handleFailure(throwable, scheduler);
            }
        }

    }
}
