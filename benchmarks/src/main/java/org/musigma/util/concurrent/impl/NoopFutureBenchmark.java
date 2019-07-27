package org.musigma.util.concurrent.impl;

import org.musigma.util.concurrent.Future;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.Executor;

public class NoopFutureBenchmark extends OpFutureBenchmark {
    @Override
    protected Future<String> next(final Executor executor, final Blackhole bh, final int count, final Future<String> input) {
        for (int i = 0; i < count; i++) {
            bh.consume(i);
        }
        bh.consume(input);
        return input;
    }
}
