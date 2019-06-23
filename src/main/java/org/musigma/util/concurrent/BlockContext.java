package org.musigma.util.concurrent;

import org.musigma.util.Thunk;

@FunctionalInterface
public interface BlockContext {
    <T> T blockOn(final Thunk<T> thunk) throws Throwable;

    static <T> T blocking(final Thunk<T> thunk) throws Throwable {
        return BlockContexts.current().blockOn(thunk);
    }
}
