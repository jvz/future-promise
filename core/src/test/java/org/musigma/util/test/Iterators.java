package org.musigma.util.test;

import org.musigma.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class Iterators {

    private Iterators() {
    }

    public static <T> Iterator<List<T>> permutationsOf(final Iterable<T> iterable) {
        return new PermutationIterator<>(iterable);
    }

    private static class PermutationIterator<T> implements Iterator<List<T>> {

        private final List<T> elements;
        private final int[] indices;
        private boolean hasNext = true;

        private PermutationIterator(final Iterable<T> iterable) {
            Map<T, Integer> map = new HashMap<>();
            List<Pair<T, Integer>> pairs = new ArrayList<>();
            for (T element : iterable) {
                int value;
                if (map.containsKey(element)) {
                    value = map.get(element);
                } else {
                    value = map.size();
                    map.put(element, value);
                }
                pairs.add(Pair.of(element, value));
            }
            pairs.sort(Comparator.comparing(Pair::getRight));
            elements = new ArrayList<>(pairs.size());
            indices = new int[pairs.size()];
            for (int i = 0; i < pairs.size(); i++) {
                Pair<T, Integer> pair = pairs.get(i);
                elements.add(pair.getLeft());
                indices[i] = pair.getRight();
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public List<T> next() {
            if (!hasNext) {
                Collections.emptyIterator().next();
            }

            List<T> result = new ArrayList<>(elements);
            int i = indices.length - 2;
            while (i >= 0 && indices[i] >= indices[i + 1]) {
                i--;
            }
            if (i < 0) {
                hasNext = false;
            } else {
                int j = indices.length - 1;
                while (indices[j] <= indices[i]) {
                    j--;
                }
                swap(i, j);

                int len = (indices.length - i) / 2;
                for (int k = 1; k <= len; k++) {
                    swap(i + k, indices.length - k);
                }
            }
            return result;
        }

        private void swap(final int i, final int j) {
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
            Collections.swap(elements, i, j);
        }
    }

    public static <T, R> Iterator<R> map(final Iterator<T> iterator, final Function<? super T, ? extends R> function) {
        return new FunctorIterator<>(iterator, function);
    }

    private static class FunctorIterator<T, R> implements Iterator<R> {
        private final Iterator<T> iterator;
        private final Function<? super T, ? extends R> function;

        private FunctorIterator(final Iterator<T> iterator, final Function<? super T, ? extends R> function) {
            this.iterator = iterator;
            this.function = function;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public R next() {
            return function.apply(iterator.next());
        }
    }

}
