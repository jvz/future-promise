package org.musigma.util.concurrent;

import org.musigma.util.Thunk;

@FunctionalInterface
public interface BlockContext {
    <T> T blockOn(final Thunk<T> thunk) throws Exception;

    static <T> T blocking(final Thunk<T> thunk) throws Exception {
        return BlockContexts.current().blockOn(thunk);
    }
}
