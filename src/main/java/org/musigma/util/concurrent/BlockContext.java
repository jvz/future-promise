package org.musigma.util.concurrent;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface BlockContext {
    <T> T blockOn(final Callable<T> thunk) throws Exception;

    static <T> T blocking(final Callable<T> thunk) throws Exception {
        return BlockContexts.current().blockOn(thunk);
    }
}
