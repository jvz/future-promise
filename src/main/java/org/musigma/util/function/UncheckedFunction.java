package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedFunction<T, R> {
    static <T> UncheckedFunction<T, T> identity() {
        return t -> t;
    }

    R apply(final T value) throws Throwable;
}
