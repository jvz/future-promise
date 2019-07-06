package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedBiFunction<T, U, R> {

    R apply(final T left, final U right) throws Exception;

}
