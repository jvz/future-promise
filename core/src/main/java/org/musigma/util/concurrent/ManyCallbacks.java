package org.musigma.util.concurrent;

import java.util.concurrent.Callable;

class ManyCallbacks<T> implements Callbacks<T> {

    private final Transformation<T, ?> head;
    private final Callbacks<T> tail;

    ManyCallbacks(final Transformation<T, ?> head, final Callbacks<T> tail) {
        this.head = head;
        this.tail = tail;
    }

    @Override
    public Callbacks<T> concat(final Callbacks<T> next) {
        return tail.concat(new ManyCallbacks<>(head, next));
    }

    @Override
    public void submitWithValue(final Callable<T> resolved) {
        head.submitWithValue(resolved);
        tail.submitWithValue(resolved);
    }
}
