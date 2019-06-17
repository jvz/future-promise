package org.musigma.util.function;

@FunctionalInterface
public interface Supplier<T> extends Function<Void, T> {
    T get() throws Throwable;

    @Override
    default T apply(final Void value) throws Throwable {
        return get();
    }
}
