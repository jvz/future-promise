package org.musigma.util.concurrent;

import org.musigma.util.Exceptions;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.Objects;

class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {

    static Transformation<?, ?> NOOP = new Transformation<>(null, Scheduler.parasitic(), null, Transform.noop);

    private UncheckedFunction<Object, Object> function;
    private Scheduler scheduler;
    private Thunk<F> argument;
    private Transform transform;

    @SuppressWarnings("unchecked")
    Transformation(final UncheckedFunction<?, ?> function, final Scheduler scheduler, final Thunk<F> argument, final Transform transform) {
        super(NOOP);
        this.function = (UncheckedFunction<Object, Object>) function;
        this.scheduler = scheduler;
        this.argument = argument;
        this.transform = transform;
    }

    private static <T> Thunk<T> resolve(final Thunk<T> value) {
        Objects.requireNonNull(value);
        // TODO: this can support the use of a ControlThrowable and similar
        return value;
    }

    void submitWithValue(final Thunk<F> resolved) {
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
        final boolean completed = tryComplete(ref.get(), resolve(Thunk.error(t)));
        if (completed && wasInterrupted) {
            Thread.currentThread().interrupt();
        }
        if (transform == Transform.onComplete || !completed) {
            scheduler.reportError(t);
        } else {
            Exceptions.rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        final Thunk<F> value = this.argument;
        this.argument = null;
        final UncheckedFunction<Object, Object> function = this.function;
        this.function = null;
        final Scheduler scheduler = this.scheduler;
        this.scheduler = null;
        try {
            Thunk<T> resolvedResult = null;
            switch (transform) {
                case noop:
                    break;

                case map: {
                    final UncheckedFunction<? super F, ? extends T> f = (UncheckedFunction<? super F, ? extends T>) function;
                    resolvedResult = resolve(value.map(f));
//                    resolvedResult = value.isSuccess() ? Thunk.value(function.apply(value.get())) : value;
                    break;
                }

                case flatMap: {
                    if (value.isSuccess()) {
                        final Object f = function.apply(value.get());
                        if (f instanceof DefaultPromise) {
                            ((DefaultPromise<T>) f).linkRootOf(this, null);
                        } else {
                            completeWith((Future<T>) f);
                        }
                    } else {
                        resolvedResult = value.recastIfError();
                    }
                    break;
                }

                case transform: {
                    resolvedResult = resolve((Thunk<T>) function.apply(value));
                    break;
                }

                case onComplete: {
                    function.apply(value);
                    break;
                }

                case filter: {
                    final UncheckedPredicate<F> predicate = (UncheckedPredicate) function;
                    resolvedResult = resolve(value.filter(predicate)).recast();
//                    resolvedResult = value.isError() || predicate.test(value.get()) ? value.recast() : Thunk.error(new NoSuchElementException("filter predicate failed"));
                    break;
                }

                default:
                    throw new UnsupportedOperationException("Unknown transformation type");
            }
            if (resolvedResult != null) {
                tryComplete(ref.get(), resolvedResult);
            }
        } catch (Throwable throwable) {
            handleFailure(throwable, scheduler);
        }
    }

}
