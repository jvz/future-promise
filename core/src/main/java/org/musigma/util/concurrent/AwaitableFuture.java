package org.musigma.util.concurrent;

import org.apiguardian.api.API;

import java.util.concurrent.Future;

@API(status = API.Status.EXPERIMENTAL)
public interface AwaitableFuture<T> extends Awaitable, Future<T> {
}
