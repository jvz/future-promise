package org.musigma.util.function;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface UncheckedSupplier<T> extends UncheckedFunction<Void, T>, Callable<T> {
    T get() throws Exception;

    @Override
    default T apply(final Void value) throws Exception {
        return get();
    }

    @Override
    default T call() throws Exception {
        return get();
    }
}
