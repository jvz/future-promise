package org.musigma.util.concurrent;

import java.util.concurrent.Callable;

interface Callbacks<T> {
    Callbacks<T> concat(final Callbacks<T> next);

    void submitWithValue(final Callable<T> resolved);
}
