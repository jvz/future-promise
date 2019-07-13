package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;
import org.musigma.util.function.UncheckedPredicate;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

@Batchable
class Transformation<F, T> extends DefaultPromise<T> implements Callbacks<F>, Runnable {

    enum Type {
        noop, map, flatMap, transform, transformWith, onComplete, recover, recoverWith, filter;

        <F, T> Transformation<F, T> using(final UncheckedFunction<?, ?> function, final Executor executor) {
            return new Transformation<>(function, executor, null, this);
        }
    }

    static Transformation<?, ?> NOOP = Type.noop.using(UncheckedFunction.identity(), Executors.parasitic());

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
        if (transformType == Type.onComplete || !completed) {
            if (executor instanceof Batching.BatchingExecutor) {
                ((Batching.BatchingExecutor) executor).reportFailure(e);
            } else {
                e.printStackTrace();
            }
        }
    }

    boolean benefitsFromBatching() {
        return transformType != Type.onComplete;
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
            // TODO: simplify resolved result logic due to lack of ControlThrowable
            Callable<T> resolvedResult = null;
            switch (transformType) {
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
                    resolvedResult = (Thunk<T>) function.apply(value);
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
                        fallback = value.recover(f);
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
            handleFailure(e, executor);
        }
    }

    @Override
    public Callbacks<F> concat(final Callbacks<F> next) {
        return new ManyCallbacks<>(this, next);
    }

}
