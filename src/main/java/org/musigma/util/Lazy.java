package org.musigma.util;

import java.util.concurrent.Callable;

public final class Lazy<T> implements Callable<T> {

    private final Callable<T> lazyValue;
    private volatile T value;

    private Lazy(final Callable<T> lazyValue) {
        this.lazyValue = lazyValue;
    }

    public static <T> Lazy<T> from(final Callable<T> callable) {
        return new Lazy<>(callable);
    }

    public static <T> Lazy<T> value(final T value) {
        return new Lazy<>(() -> value);
    }

    @Override
    public T call() throws Exception {
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    value = lazyValue.call();
                }
            }
        }
        return value;
    }
}
