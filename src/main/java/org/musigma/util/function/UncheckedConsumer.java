package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedConsumer<T> extends UncheckedFunction<T, Void> {
    void accept(final T value) throws Throwable;

    @Override
    default Void apply(final T value) throws Throwable {
        accept(value);
        return null;
    }
}
