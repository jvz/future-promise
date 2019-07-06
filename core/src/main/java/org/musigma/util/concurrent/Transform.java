package org.musigma.util.concurrent;

import org.musigma.util.function.UncheckedFunction;

enum Transform {
    noop, map, flatMap, transform, transformWith, onComplete, recover, recoverWith, filter;

    <F, T> Transformation<F, T> using(final UncheckedFunction<?, ?> function, final Scheduler scheduler) {
        return new Transformation<>(function, scheduler, null, this);
    }
}
