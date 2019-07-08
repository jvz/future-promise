package org.musigma.util.concurrent;

import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedFunction;

public class BlockContexts {

    private static final ThreadLocal<BlockContext> LOCAL_CONTEXT = new ThreadLocal<>();

    private static BlockContext prefer(final BlockContext candidate) {
        if (candidate != null) {
            return candidate;
        }
        Thread thread = Thread.currentThread();
        return thread instanceof BlockContext ? (BlockContext) thread : BlockContext.DEFAULT;
    }

    // TODO: move to BlockContext and make this package-private
    public static BlockContext current() {
        return prefer(LOCAL_CONTEXT.get());
    }

    public static <T> T withBlockContext(final BlockContext context, final Thunk<T> thunk) throws Exception {
        final BlockContext previous = LOCAL_CONTEXT.get();
        if (previous == context) {
            return thunk.call();
        }
        LOCAL_CONTEXT.set(context);
        try {
            return thunk.call();
        } finally {
            LOCAL_CONTEXT.set(previous);
        }
    }

    public static <T> T usingBlockContext(final BlockContext context, final UncheckedFunction<BlockContext, T> function) throws Exception {
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
