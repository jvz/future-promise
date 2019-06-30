package org.musigma.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Awaitable {
    void await() throws InterruptedException;

    void await(final long time, final TimeUnit unit) throws InterruptedException, TimeoutException;
}
