package org.musigma.util.concurrent;

import org.apiguardian.api.API;

import java.util.concurrent.Callable;

@API(status = API.Status.EXPERIMENTAL)
public interface PromiseFactory {
    static PromiseFactory getInstance() {
        return PromiseFactoryHolder.INSTANCE.factory;
    }

    <T> Promise<T> newIncompletePromise();

    <T> Promise<T> newCompletePromise(final Callable<T> callable);

}
