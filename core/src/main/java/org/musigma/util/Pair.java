package org.musigma.util;

import java.util.Map;

public final class Pair<A, B> implements Map.Entry<A, B> {

    private final A left;
    private final B right;

    private Pair(final A left, final B right) {
        this.left = left;
        this.right = right;
    }

    public static <A, B> Pair<A, B> of(final A left, final B right) {
        return new Pair<>(left, right);
    }

    @Override
    public A getKey() {
        return left;
    }

    @Override
    public B getValue() {
        return right;
    }

    public A getLeft() {
        return left;
    }

    public B getRight() {
        return right;
    }

    @Override
    public B setValue(final B value) {
        throw new UnsupportedOperationException("immutable");
    }
}
