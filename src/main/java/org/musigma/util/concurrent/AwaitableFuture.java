package org.musigma.util.concurrent;

import java.util.concurrent.Future;

public interface AwaitableFuture<T> extends Awaitable, Future<T> {
}
