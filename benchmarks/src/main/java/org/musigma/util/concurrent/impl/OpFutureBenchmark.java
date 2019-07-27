package org.musigma.util.concurrent.impl;

import org.musigma.util.Thunk;
import org.musigma.util.concurrent.Future;
import org.musigma.util.concurrent.Promise;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class OpFutureBenchmark extends AbstractFutureBenchmark {
    protected final Thunk<String> failedThunk = Thunk.error(new Exception("some failed code"));
    protected final Thunk<String> successfulThunk = Thunk.value("some successful value");
    protected final Promise<String> failedPromise = Promise.from(failedThunk);
    protected final Promise<String> successfulPromise = Promise.from(successfulThunk);

    protected final boolean await(final Future<?> future) throws TimeoutException, InterruptedException {
        return future.getCurrent().isPresent() || future.await(1, TimeUnit.MINUTES) == future;
    }

    protected abstract Future<String> next(final Executor executor, final Blackhole bh, final int count, final Future<String> input);

    @Benchmark
    public boolean pre(final Blackhole bh) throws TimeoutException, InterruptedException {
        return await(next(executor, bh, recursion, successfulPromise.future()));
    }

    @Benchmark
    public boolean post(final Blackhole bh) throws TimeoutException, InterruptedException {
        final Promise<String> promise = Promise.newPromise();
        final Future<String> future = next(executor, bh, recursion, promise.future());
        promise.complete(successfulThunk);
        return await(future);
    }
}
