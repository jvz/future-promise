package org.musigma.util.concurrent;

import org.apiguardian.api.API;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@API(status = API.Status.EXPERIMENTAL)
public interface Awaitable {

    void await() throws InterruptedException;

    void await(final long time, final TimeUnit unit) throws InterruptedException, TimeoutException;

    static <F extends Awaitable> F await(final F future) throws InterruptedException {
        future.await();
        return future;
    }

    static <F extends Awaitable> F await(final F future, final long time, final TimeUnit unit) throws TimeoutException, InterruptedException {
        future.await(time, unit);
        return future;
    }

}
