package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.Failure;
import org.musigma.util.Success;
import org.musigma.util.Try;
import org.musigma.util.function.Function;
import org.musigma.util.function.Predicate;

import java.util.NoSuchElementException;
import java.util.Objects;

class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {

    static Transformation<?, ?> NOOP = new Transformation<>(null, Scheduler.inline(), null, Transform.noop);

    private Function<Object, Object> function;
    private Scheduler scheduler;
    private Try<F> argument;
    private Transform transform;

    @SuppressWarnings("unchecked")
    Transformation(final Function<?, ?> function, final Scheduler scheduler, final Try<F> argument, final Transform transform) {
        super(NOOP);
        this.function = (Function<Object, Object>) function;
        this.scheduler = scheduler;
        this.argument = argument;
        this.transform = transform;
    }

    private static <T> Try<T> resolve(final Try<T> value) {
        Objects.requireNonNull(value);
        // TODO: this can support the use of a ControlThrowable and similar
        return value;
    }

    void submitWithValue(final Try<F> resolved) {
        argument = resolved;
        try {
            scheduler.execute(this);
        } catch (final Throwable t) {
            final Scheduler scheduler = this.scheduler;
            this.scheduler = null;
            function = null;
            argument = null;
            handleFailure(t, scheduler);
        }
    }

    private void handleFailure(final Throwable t, final Scheduler scheduler) {
        final boolean wasInterrupted = t instanceof InterruptedException;
        if (!wasInterrupted) {
            Exceptions.rethrowIfFatal(t);
        }
        final boolean completed = tryComplete(ref.get(), resolve(new Failure<>(t)));
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
