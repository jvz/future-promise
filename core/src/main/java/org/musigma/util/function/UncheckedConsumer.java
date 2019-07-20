package org.musigma.util.function;

import org.apiguardian.api.API;

@FunctionalInterface
@API(status = API.Status.EXPERIMENTAL)
public interface UncheckedConsumer<T> extends UncheckedFunction<T, Void> {
    void accept(final T value) throws Exception;

    @Override
    default Void apply(final T value) throws Exception {
        accept(value);
        return null;
    }
}
