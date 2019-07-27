package org.musigma.util.concurrent.impl;

import org.musigma.util.concurrent.Future;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.Executor;

public class MapFutureBenchmark extends OpFutureBenchmark {
    @Override
    protected Future<String> next(final Executor executor, final Blackhole bh, final int count, final Future<String> input) {
        Future<String> result = input;
        for (int i = 0; i < count; i++) {
            result = result.map(executor, s -> {
                bh.consume(s);
                return s;
            });
        }
        return result;
    }
}
