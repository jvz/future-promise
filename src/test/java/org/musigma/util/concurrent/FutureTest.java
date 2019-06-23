package org.musigma.util.concurrent;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class FutureTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    Future<String> testAsync(final String s, final Scheduler scheduler) {
        switch (s) {
            case "Hello":
                return Future.fromAsync(() -> "World", scheduler);
            case "Failure":
                return Future.failed(new RuntimeException("expected exception"));
            case "NoReply":
                return Promise.<String>newPromise().future();
            default:
                throw new IllegalArgumentException(s);
        }
    }

    <T> T fail(final String msg) {
        throw new AssertionError(msg);
    }

    <T> Future<T> failAsync(final String msg) {
        return Future.failed(new AssertionError(msg));
    }

    <T> T schedulerNotUsed(final UncheckedFunction<Scheduler, T> function) throws Exception {
        final Promise<Runnable> p = Promise.newPromise();
        final Scheduler unusedScheduler = new Scheduler() {
            @Override
            public void execute(final Runnable runnable) {
                p.success(runnable);
            }

            @Override
            public void reportFailure(final Throwable t) {
                p.failure(t);
            }
        };
        T t = function.apply(unusedScheduler);
        assertFalse("Future should not execute anything", p.future().getCurrent().isPresent());
        return t;
    }

    void schedulerNotUsedV(final UncheckedConsumer<Scheduler> consumer) throws Exception {
        schedulerNotUsed(consumer);
    }

    @Test
    public void testSuccessful() throws ExecutionException, InterruptedException {
        Future<String> f = Future.successful("test");
        assertEquals("test", f.get());
    }

    @Test
    public void testFailure() throws ExecutionException, InterruptedException {
        Future<Object> failed = Future.failed(new IllegalStateException());
        expectedException.expect(IllegalStateException.class);
        failed.get();
    }

    @Test
    public void testMap() throws ExecutionException, InterruptedException {
        Future<Integer> size = Future.successful("hello").map(String::length);
        assertEquals(5, (int) size.get());
    }

    @Test
    public void testFlatMap() throws ExecutionException, InterruptedException {
        Future<Integer> size = Future.successful("hello").flatMap(s -> Future.successful(s.length()));
        assertEquals(5, (int) size.get());
    }

    @Test
    public void testFilter() throws ExecutionException, InterruptedException {
        assertNotNull(Future.successful("foo").filter(Objects::nonNull).get());
        expectedException.expect(IllegalStateException.class);
        Future.failed(new IllegalStateException()).filter(ignored -> true).get();
    }

    @Test
    public void testTransform() throws ExecutionException, InterruptedException {
        Future<Integer> testLength = Future.successful("test").transform(result -> result.map(String::length));
        assertEquals(4, (int) testLength.get());
        Future<String> resultFuture = Future.successful("test").transform(ignored -> Thunk.error(new IllegalStateException()));
        expectedException.expect(IllegalStateException.class);
        resultFuture.get();
    }

    @Test
    public void testVoid() throws Exception {
        assertNotNull(Future.VOID);
        assertSame(Future.VOID, Future.VOID);
        assertTrue(Future.VOID.isDone());
        assertNull(Future.VOID.get());
    }

    @Test
    public void testNever() throws Exception {
        assertNotNull(Future.never());
        Future<Void> never = Future.never();
        assertSame(never, Future.never());
        assertFalse(never.isDone());
        assertFalse(never.getCurrent().isPresent());
        schedulerNotUsedV(s -> never.onComplete(ignored -> fail("should not execute onComplete"), s));
        assertSame(never, schedulerNotUsed(s -> never.transform(UncheckedFunction.identity(), s)));
        assertSame(never, schedulerNotUsed(s -> never.map(UncheckedFunction.identity(), s)));
        assertSame(never, schedulerNotUsed(s -> never.flatMap(ignored -> failAsync("flatMap should not be called"))));
        assertSame(never, schedulerNotUsed(s -> never.filter(ignored -> fail("should not execute filter"))));
    }

    @Test
    public void testDefaultSchedulerReportsUncaughtExceptions() throws ExecutionException, InterruptedException {
        Promise<Throwable> p = Promise.newPromise();
        SchedulerExecutorService ses = SchedulerExecutorService.fromExecutorService(null, p::trySuccess);
        RuntimeException e = new RuntimeException();
        try {
            ses.execute(() -> {
                throw e;
            });
            assertSame(e, p.future().get());
        } finally {
            ses.shutdown();
        }
    }
}