package org.musigma.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

class Link<T> {

    final AtomicReference<DefaultPromise<T>> ref;

    Link(final DefaultPromise<T> to) {
        ref = new AtomicReference<>(to);
    }

    @SuppressWarnings("unchecked")
    DefaultPromise<T> promise(final DefaultPromise<T> owner) {
        DefaultPromise<T> current = ref.get();
        DefaultPromise<T> target = current;
        while (true) {
            final Object value = target.ref.get();
            if (value instanceof Callbacks) {
                if (ref.compareAndSet(current, target)) {
                    return target; // linked
                } else {
                    current = ref.get();
                }
            } else if (value instanceof Link) {
                target = ((Link<T>) value).ref.get();
            } else {
                owner.unlink((Callable<T>) value);
                return owner;
            }
        }
    }

}
