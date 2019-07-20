package org.musigma.util.function;

import org.apiguardian.api.API;

@FunctionalInterface
@API(status = API.Status.EXPERIMENTAL)
public interface UncheckedBiFunction<T, U, R> {

    R apply(final T left, final U right) throws Exception;

}
