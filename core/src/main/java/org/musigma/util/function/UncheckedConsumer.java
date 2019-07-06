package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedConsumer<T> extends UncheckedFunction<T, Void> {
    void accept(final T value) throws Exception;

    @Override
    default Void apply(final T value) throws Exception {
        accept(value);
        return null;
    }
}
