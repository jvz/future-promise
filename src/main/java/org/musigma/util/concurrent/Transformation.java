package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Callable;

@Batchable
class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {

    static Transformation<?, ?> NOOP = Transform.noop.using(UncheckedFunction.identity(), Scheduler.parasitic());

    private UncheckedFunction<Object, Object> function;
    private Scheduler scheduler;
    private Callable<F> argument;
    private Transform transform;

    @SuppressWarnings("unchecked")
    Transformation(final UncheckedFunction<?, ?> function, final Scheduler scheduler, final Callable<F> argument, final Transform transform) {
        super();
        this.function = (UncheckedFunction<Object, Object>) function;
        this.scheduler = scheduler;
        this.argument = argument;
        this.transform = transform;
    }

    private static <T> Callable<T> resolve(final Callable<T> value) {
        Objects.requireNonNull(value);
        // TODO: this can support the use of a ControlThrowable and similar
        return value;
    }

    @Override
    public void submitWithValue(final Callable<F> resolved) {
        argument = resolved;
        try {
            scheduler.execute(this);
        } catch (final Exception e) {
            final Scheduler scheduler = this.scheduler;
            this.scheduler = null;
            function = null;
            argument = null;
            handleFailure(e, scheduler);
        }
    }

    private void handleFailure(final Exception e, final Scheduler scheduler) {
        final boolean interrupted = e instanceof InterruptedException;
        final boolean completed = tryComplete(ref.get(), resolve(Thunk.error(e)));
        if (completed && interrupted) {
            Thread.currentThread().interrupt();
        }
        if (transform == Transform.onComplete || !completed) {
            scheduler.reportFailure(e);
        }
    }

    boolean benefitsFromBatching() {
        return transform != Transform.onComplete;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        final Thunk<F> value = Thunk.from(this.argument);
        this.argument = null;
        final UncheckedFunction<Object, Object> function = this.function;
        this.function = null;
        final Scheduler scheduler = this.scheduler;
        this.scheduler = null;
        try {
            Callable<T> resolvedResult = null;
            switch (transform) {
                case noop:
                    break;

                case map: {
                    final UncheckedFunction<? super F, ? extends T> f = (UncheckedFunction<? super F, ? extends T>) function;
                    resolvedResult = value.isSuccess() ? Thunk.value(f.apply(value.call())) : value.recast();
                    break;
                }

                case flatMap: {
                    if (value.isSuccess()) {
                        final Object f = function.apply(value.call());
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

                case transformWith: {
                    final UncheckedFunction<Thunk<F>, Future<T>> f = (UncheckedFunction) function;
                    final Future<T> future = f.apply(value);
                    if (future instanceof DefaultPromise) {
                        ((DefaultPromise<T>) future).linkRootOf(this, null);
                    } else {
                        completeWith(future);
                    }
                }

                case onComplete: {
                    function.apply(value);
                    break;
                }

                case recover: {
                    // F == T
                    Callable<F> fallback;
                    if (value.isSuccess()) {
                        fallback = value;
                    } else {
                        final UncheckedFunction<Exception, F> f = (UncheckedFunction) function;
                        fallback = resolve(value.recover(f));
                    }
                    resolvedResult = Thunk.from(fallback).recast();
                    break;
                }

                case recoverWith: {
                    // F == T
                    if (value.isSuccess()) {
                        resolvedResult = value.recast();
                    } else {
                        final UncheckedFunction<Exception, Future<T>> f = (UncheckedFunction) function;
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
                    final UncheckedPredicate<F> predicate = (UncheckedPredicate) function;
                    resolvedResult = value.isError() || predicate.test(value.call()) ? value.recast() : Thunk.error(new NoSuchElementException("filter predicate failed"));
                    break;
                }

                default:
                    throw new UnsupportedOperationException("Unknown transformation type");
            }
            if (resolvedResult != null) {
                tryComplete(ref.get(), resolvedResult);
            }
        } catch (Exception e) {
            handleFailure(e, scheduler);
        }
    }

    @Override
    public Callbacks<F> concat(final Callbacks<F> next) {
        return new ManyCallbacks<>(this, next);
    }
}
