package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedSupplier<T> extends UncheckedFunction<Void, T> {
    T get() throws Exception;

    @Override
    default T apply(final Void value) throws Exception {
        return get();
    }
}
