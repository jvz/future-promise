package org.musigma.util.concurrent;

import org.musigma.util.Try;

import java.util.concurrent.atomic.AtomicReference;

class Link<T> {
    final AtomicReference<DefaultPromise<T>> ref;

    Link(final DefaultPromise<T> to) {
        ref = new AtomicReference<>(to);
    }

    DefaultPromise<T> promise(final DefaultPromise<T> owner) {
        final DefaultPromise<T> current = ref.get();
        return compressed(current, current, owner);
    }

    @SuppressWarnings("unchecked")
    private DefaultPromise<T> compressed(final DefaultPromise<T> current, final DefaultPromise<T> target, final DefaultPromise<T> owner) {
        final Object value = target.ref.get();
        if (value instanceof Callbacks) {
            if (ref.compareAndSet(current, target)) {
                return target; // linked
            } else {
                return compressed(ref.get(), target, owner); // retry
            }
        } else if (value instanceof Link) {
            return compressed(current, ((Link<T>) value).ref.get(), owner);
        } else {
            owner.unlink((Try<T>) value);
            return owner;
        }
    }
}
