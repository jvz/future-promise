package org.musigma.util.function;

@FunctionalInterface
public interface Function<T, R> {
    R apply(final T value) throws Throwable;
}
