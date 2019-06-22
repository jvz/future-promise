package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedFunction<T, R> {
    R apply(final T value) throws Throwable;
}
