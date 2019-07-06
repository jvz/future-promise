package org.musigma.util.concurrent;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface BlockContext {

    BlockContext DEFAULT = Callable::call;

    static <T> T blocking(final Callable<T> thunk) throws Exception {
        return BlockContexts.current().blockOn(thunk);
    }

    <T> T blockOn(final Callable<T> thunk) throws Exception;

}
