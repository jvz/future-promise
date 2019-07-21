package org.musigma.util.concurrent;

import org.musigma.util.concurrent.impl.DefaultPromiseFactory;

enum PromiseFactoryHolder {
    INSTANCE;

    final PromiseFactory factory;

    PromiseFactoryHolder() {
        PromiseFactory factory;
        try {
            String factoryClassName = System.getProperty(PromiseFactory.class.getName(), DefaultPromiseFactory.class.getName());
            factory = Thread.currentThread()
                    .getContextClassLoader()
                    .loadClass(factoryClassName)
                    .asSubclass(PromiseFactory.class)
                    .newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | SecurityException e) {
            factory = new DefaultPromiseFactory();
        }
        this.factory = factory;
    }
}
