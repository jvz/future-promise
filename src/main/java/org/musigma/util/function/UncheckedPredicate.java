package org.musigma.util.function;

@FunctionalInterface
public interface UncheckedPredicate<T> extends UncheckedFunction<T, Boolean> {
    boolean test(final T value) throws Throwable;

    @Override
    default Boolean apply(final T value) throws Throwable {
        return test(value);
    }
}
