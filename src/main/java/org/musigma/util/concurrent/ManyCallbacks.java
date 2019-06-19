package org.musigma.util.concurrent;

class ManyCallbacks<T> implements Callbacks<T> {
    final Transformation<T, ?> head;
    final Callbacks<T> tail;

    ManyCallbacks(final Transformation<T, ?> head, final Callbacks<T> tail) {
        this.head = head;
        this.tail = tail;
    }
}
