package org.musigma.util.concurrent;

import org.junit.jupiter.api.Test;
import org.musigma.util.Thunk;
import org.musigma.util.function.UncheckedConsumer;
import org.musigma.util.function.UncheckedFunction;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.musigma.util.test.Assertions.assertThrowsWrapped;

class FutureTest {

    <T> T fail(final String msg) {
        throw new AssertionError(msg);
    }

    <T> Future<T> failAsync(final String msg) {
        return Future.failed(new RuntimeException(msg));
    }

    <T> T executorNotUsed(final UncheckedFunction<Executor, T> function) throws Exception {
        final Promise<Runnable> p = Promise.newPromise();
        T t = function.apply(p::success);
        assertFalse(p.future().getCurrent().isPresent(), "Future should not execute anything");
        return t;
    }

    void executorNotUsedV(final UncheckedConsumer<Executor> consumer) throws Exception {
        executorNotUsed(consumer);
    }

    @Test
    void testSuccessful() throws ExecutionException, InterruptedException {
        Future<String> f = Future.successful("test");
        assertEquals("test", f.get());
    }

    @Test
    void testFailure() {
        Future<Object> failed = Future.failed(new IllegalStateException("failure"));
        assertThrowsWrapped(IllegalStateException.class, failed::get, "failure");
    }

    @Test
    void testMap() throws ExecutionException, InterruptedException {
        Future<Integer> size = Future.successful("hello").map(String::length);
        assertEquals(5, (int) size.get());
    }

    @Test
    void testFlatMap() throws ExecutionException, InterruptedException {
        Future<Integer> size = Future.successful("hello").flatMap(s -> Future.successful(s.length()));
        assertEquals(5, (int) size.get());
    }

    @Test
    void testFilter() throws ExecutionException, InterruptedException {
        assertNotNull(Future.successful("foo").filter(Objects::nonNull).get());
        assertThrowsWrapped(IllegalStateException.class, () -> Future.failed(new IllegalStateException("error")).filter(ignored -> true).get(), "error");
    }

    @Test
    void testTransform() throws ExecutionException, InterruptedException {
        Future<Integer> testLength = Future.successful("test").transform(result -> result.map(String::length));
        assertEquals(4, (int) testLength.get());
        Future<String> resultFuture = Future.successful("test").transform(ignored -> Thunk.error(new IllegalStateException("uh-oh")));
        assertThrowsWrapped(IllegalStateException.class, resultFuture::get, "uh-oh");
    }

    @Test
    void testVoid() throws Exception {
        assertNotNull(Future.VOID);
        assertSame(Future.VOID, Future.VOID);
        assertTrue(Future.VOID.isDone());
        assertNull(Future.VOID.get());
    }

    @Test
    void testNever() throws Exception {
        assertNotNull(Future.never());
        Future<Void> never = Future.never();
        assertSame(never, Future.never());
        assertFalse(never.isDone());
        assertFalse(never.getCurrent().isPresent());
        executorNotUsedV(s -> never.onComplete(ignored -> fail("should not execute onComplete"), s));
        assertSame(never, executorNotUsed(s -> never.transform(UncheckedFunction.identity(), s)));
        assertSame(never, executorNotUsed(s -> never.map(UncheckedFunction.identity(), s)));
        assertSame(never, executorNotUsed(s -> never.flatMap(ignored -> failAsync("flatMap should not be called"))));
        assertSame(never, executorNotUsed(s -> never.filter(ignored -> fail("should not execute filter"))));
    }

}