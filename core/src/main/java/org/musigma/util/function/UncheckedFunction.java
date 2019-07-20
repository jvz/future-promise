package org.musigma.util.function;

import org.apiguardian.api.API;

@FunctionalInterface
@API(status = API.Status.EXPERIMENTAL)
public interface UncheckedFunction<T, R> {
    static <T> UncheckedFunction<T, T> identity() {
        return t -> t;
    }

    R apply(final T value) throws Exception;
}
