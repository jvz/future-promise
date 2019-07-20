package org.musigma.util.function;

import org.apiguardian.api.API;

@FunctionalInterface
@API(status = API.Status.EXPERIMENTAL)
public interface UncheckedPredicate<T> extends UncheckedFunction<T, Boolean> {
    boolean test(final T value) throws Exception;

    @Override
    default Boolean apply(final T value) throws Exception {
        return test(value);
    }
}
