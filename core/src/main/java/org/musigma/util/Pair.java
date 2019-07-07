package org.musigma.util;

import java.util.Map;
import java.util.Objects;

public final class Pair<A, B> implements Map.Entry<A, B> {

    private final A left;
    private final B right;

    private Pair(final A left, final B right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Constructs an immutable ordered pair with the given values.
     */
    public static <A, B> Pair<A, B> of(final A left, final B right) {
        return new Pair<>(left, right);
    }

    /**
     * Constructs an immutable ordered pair from an existing Map Entry.
     */
    public static <A, B> Pair<A, B> from(final Map.Entry<A, B> entry) {
        return entry instanceof Pair ? (Pair<A, B>) entry : new Pair<>(entry.getKey(), entry.getValue());
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left) &&
                Objects.equals(right, pair.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return "Pair{" + left + ", " + right + '}';
    }

}
