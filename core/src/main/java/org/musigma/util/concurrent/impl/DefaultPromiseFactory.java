package org.musigma.util.concurrent.impl;

import org.musigma.util.concurrent.Promise;
import org.musigma.util.concurrent.PromiseFactory;

import java.util.concurrent.Callable;

public class DefaultPromiseFactory implements PromiseFactory {
    @Override
    public <T> Promise<T> newIncompletePromise() {
        return new DefaultPromise<>();
    }

    @Override
    public <T> Promise<T> newCompletePromise(final Callable<T> callable) {
        return new DefaultPromise<>(callable);
    }
}
