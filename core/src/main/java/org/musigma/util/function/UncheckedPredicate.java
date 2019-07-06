package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedPredicate<T> extends UncheckedFunction<T, Boolean> {
    boolean test(final T value) throws Exception;

    @Override
    default Boolean apply(final T value) throws Exception {
        return test(value);
    }
}
