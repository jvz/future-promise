package org.musigma.util.concurrent;

import org.musigma.util.function.UncheckedFunction;

import java.util.concurrent.Callable;

class BlockContexts {

    private static final ThreadLocal<BlockContext> LOCAL_CONTEXT = new ThreadLocal<>();

    private static BlockContext prefer(final BlockContext candidate) {
        if (candidate != null) {
            return candidate;
        }
        Thread thread = Thread.currentThread();
        return thread instanceof BlockContext ? (BlockContext) thread : BlockContext.DEFAULT;
    }

    static BlockContext current() {
        return prefer(LOCAL_CONTEXT.get());
    }

    static <T> T withBlockContext(final BlockContext context, final Callable<T> callable) throws Exception {
        final BlockContext previous = LOCAL_CONTEXT.get();
        if (previous == context) {
            return callable.call();
        }
        LOCAL_CONTEXT.set(context);
        try {
            return callable.call();
        } finally {
            LOCAL_CONTEXT.set(previous);
        }
    }

    static <T> T usingBlockContext(final BlockContext context, final UncheckedFunction<? super BlockContext, ? extends T> function) throws Exception {
        final BlockContext previous = LOCAL_CONTEXT.get();
        if (previous == context) {
            return function.apply(prefer(previous));
        }
        LOCAL_CONTEXT.set(context);
        try {
            return function.apply(prefer(previous));
        } finally {
            LOCAL_CONTEXT.set(previous);
        }
    }

}
