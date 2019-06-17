package org.musigma.util.function;

@FunctionalInterface
public interface Consumer<T> extends Function<T, Void> {
    void accept(final T value) throws Throwable;

    @Override
    default Void apply(final T value) throws Throwable {
        accept(value);
        return null;
    }
}
