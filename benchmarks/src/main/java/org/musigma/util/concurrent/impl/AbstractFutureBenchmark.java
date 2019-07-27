package org.musigma.util.concurrent.impl;

import org.musigma.util.concurrent.Executors;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// TODO: these should be at least 10x higher values for production benchmarks
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xmx1G", "-Xms1G", "-server", "-XX:+AggressiveOpts", "-XX:+UseCompressedOops", "-XX:+AlwaysPreTouch", "-XX:+UseCondCardMark"})
@Threads(1)
public abstract class AbstractFutureBenchmark {

    @Param({"fjp", "fix", "fie", "gbl"})
    public String pool;

    @Param("1")
    public int threads;

    @Param("1024")
    public int recursion;

    protected Executor executor;
    protected ExecutorService executorService;

    @Setup
    public void setup() {
        switch (pool) {
            case "fjp":
                executorService = new ForkJoinPool(threads);
                executor = new Batching.AsynchronousBatchingExecutor(executorService, Throwable::printStackTrace);
                break;

            case "fix":
                executorService = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS, new LinkedTransferQueue<>());
                executor = new Batching.AsynchronousBatchingExecutor(executorService, Throwable::printStackTrace);
                break;

            case "fie":
                executor = Executors.parasitic();
                executorService = null;
                break;

            case "gbl":
                System.setProperty("org.musigma.util.concurrent.minThreads", Integer.toString(threads));
                System.setProperty("org.musigma.util.concurrent.numThreads", Integer.toString(threads));
                System.setProperty("org.musigma.util.concurrent.maxThreads", Integer.toString(threads));
                executor = Executors.common();
                executorService = null;
                break;

            default:
                throw new UnsupportedOperationException("invalid pool option: " + pool);
        }
    }

    @TearDown
    public void teardown() throws InterruptedException {
        if (executorService != null) {
            try {
                executorService.shutdown();
            } finally {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

}
