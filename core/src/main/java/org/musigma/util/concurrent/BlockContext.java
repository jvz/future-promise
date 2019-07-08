package org.musigma.util.concurrent;

import org.musigma.util.function.UncheckedFunction;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface BlockContext {

    BlockContext DEFAULT = Callable::call;

    /**
     * Executes a blocking Callable within the current BlockContext and returns its result.
     */
    static <T> T blocking(final Callable<T> thunk) throws Exception {
        return current().blockOn(thunk);
    }

    /**
     * Returns the current BlockContext.
     */
    static BlockContext current() {
        return BlockContexts.current();
    }

    /**
     * Executes a Callable inside this context and returns its result.
     */
    <T> T blockOn(final Callable<T> thunk) throws Exception;

    /**
     * Executes a Callable inside this BlockContext and returns its result.
     */
    default <T> T call(final Callable<? extends T> callable) throws Exception {
        return BlockContexts.withBlockContext(this, callable);
    }

    /**
     * Executes a function on the current BlockContext inside this BlockContext and returns its result.
     */
    default <T> T using(final UncheckedFunction<? super BlockContext, ? extends T> function) throws Exception {
        return BlockContexts.usingBlockContext(this, function);
    }
}
