package org.musigma.util.concurrent;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.opentest4j.AssertionFailedError;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.musigma.util.test.Assertions.assertThrowsWrapped;

class PromiseTest {

    @Test
    void emptyPromiseShouldNotBeDone() {
        Promise<Void> p = Promise.newPromise();
        assertFalse(p.future().isDone());
        assertFalse(p.isDone());
    }

    @Test
    void emptyPromiseShouldHaveEmptyCurrentValue() {
        Promise<Void> p = Promise.newPromise();
        assertFalse(p.future().getCurrent().isPresent());
        assertFalse(p.isDone());
    }

    @Test
    void emptyPromiseShouldReturnSuppliedValueOnTimeout() throws ExecutionException, InterruptedException {
        Future<String> failure = Future.failed(new RuntimeException("failure"));
        Future<String> error = Future.failed(new IllegalArgumentException("error"));
        Future<String> empty = Promise.<String>newPromise().future();
        Future<String> timeout = Future.successful("timeout");

        assertEquals("timeout", failure.fallbackTo(timeout).get());
        assertEquals("timeout", timeout.fallbackTo(empty).get());
        assertEquals("timeout", error.fallbackTo(failure).fallbackTo(timeout).get());
        assertThrowsWrapped(IllegalArgumentException.class, () -> failure.fallbackTo(error).get(), "error");
    }

    @Test
    void emptyPromiseShouldBeCompletableWithCompletedPromise() throws ExecutionException, InterruptedException {
        Promise<String> p1 = Promise.newPromise();
        p1.completeWith(Future.successful("foo"));
        assertEquals("foo", p1.future().get());
        Promise<String> p2 = Promise.newPromise();
        p2.completeWith(Future.failed(new RuntimeException("failed")));
        assertThrowsWrapped(RuntimeException.class, () -> p2.future().get(), "failed");
    }

    @Test
    void successfulPromiseShouldNotBeCompletable() throws ExecutionException, InterruptedException {
        Promise<String> p = Promise.successful("truth");
        p.completeWith(Future.successful("false"));
        assertEquals("truth", p.future().get());
    }

    @TestFactory
    List<DynamicTest> successfulPromiseTests() {
        String result = "test string";
        Promise<String> promise = Promise.successful(result);
        assertTrue(promise.isDone());
        return futureWithResult(promise.future(), result);
    }

    private static List<DynamicTest> futureWithResult(final Future<String> future, final String result) {
        return Arrays.asList(
                dynamicTest("shouldBeDone", () ->
                        assertTrue(future.isDone())),
                dynamicTest("shouldContainValue", () ->
                        assertEquals(result, future.getCurrent().orElseThrow(AssertionFailedError::new).call())),
                dynamicTest("shouldReturnWhenReady", () ->
                        assertTrue(Awaitable.await(future).isDone())),
                dynamicTest("shouldReturnResultWithGet", () ->
                        assertEquals(result, future.get())),
                dynamicTest("shouldNotTimeout", () ->
                        future.get(1, TimeUnit.NANOSECONDS)),
                dynamicTest("shouldFilterResult", () ->
                        assertAll(
                                () -> assertEquals(result, future.filter(ignored -> true).get()),
                                () -> assertThrowsWrapped(NoSuchElementException.class, () -> future.filter(ignored -> false).get(), "predicate failed for " + result))),
                dynamicTest("shouldTransformResultWithMap", () ->
                        assertEquals(result.length(), future.map(Object::toString).map(String::length).get())),
                dynamicTest("shouldComposeWithFlatMap", () ->
                        assertEquals(result + "foo", future.flatMap(o -> Future.successful("foo").map(s -> o + s)).get())),
                dynamicTest("shouldPerformActionOnComplete", () -> {
                    Promise<String> p = Promise.newPromise();
                    future.onComplete(p::complete);
                    assertEquals(result, p.future().get());
                }),
                dynamicTest("shouldNotRecoverFromException", () ->
                        assertEquals(result, future.recover(ignored -> "hello world").get()))
        );
    }

    @Test
    void failedPromiseShouldNotBeCompletable() {
        String errorMessage = "error message";
        Promise<String> p = Promise.failed(new RuntimeException(errorMessage));
        p.completeWith(Future.failed(new Exception("other error")));
        assertThrowsWrapped(RuntimeException.class, p.future()::get, errorMessage);
    }

    @TestFactory
    List<DynamicTest> failedPromiseTests() {
        String errorMessage = "threw an exception";
        Promise<String> p = Promise.failed(new RuntimeException(errorMessage));
        assertTrue(p.isDone());
        return futureWithError(p.future(), RuntimeException.class, errorMessage);
    }

    @TestFactory
    List<DynamicTest> interruptedPromiseTests() {
        String errorMessage = "interrupted by user input";
        Promise<String> p = Promise.failed(new InterruptedException(errorMessage));
        assertTrue(p.isDone());
        return futureWithError(p.future(), InterruptedException.class, errorMessage);
    }

    private static List<DynamicTest> futureWithError(final Future<String> future, final Class<? extends Exception> errorType, final String errorMessage) {
        return Arrays.asList(
                dynamicTest("shouldBeDone", () ->
                        assertTrue(future.isDone())),
                dynamicTest("shouldContainValue", () -> {
                    Exception error = future.getCurrent().orElseThrow(AssertionFailedError::new).error();
                    assertAll(
                            () -> assertSame(errorType, error.getClass()),
                            () -> assertEquals(errorMessage, error.getMessage()));
                }),
                dynamicTest("shouldNotThrowExceptionWhenReady", () ->
                        assertTrue(Awaitable.await(future).isDone())),
                dynamicTest("shouldThrowWrappedExceptionWithGet", () ->
                        assertThrowsWrapped(errorType, future::get, errorMessage)),
                dynamicTest("shouldRetainExceptionWithFilter", () ->
                        assertAll(
                                () -> assertThrowsWrapped(errorType, future.filter(ignored -> true)::get, errorMessage),
                                () -> assertThrowsWrapped(errorType, future.filter(ignored -> false)::get, errorMessage))),
                dynamicTest("shouldRetainExceptionWithMap", () ->
                        assertThrowsWrapped(errorType, future.map(String::length)::get, errorMessage)),
                dynamicTest("shouldRetainExceptionWithFlatMap", () ->
                        assertThrowsWrapped(errorType, future.flatMap(o -> Future.successful(o.length()))::get, errorMessage)),
                dynamicTest("shouldRecoverFromException", () ->
                        assertEquals(errorMessage, future.recover(Exception::getMessage).get())),
                dynamicTest("shouldPerformActionOnException", () -> {
                    Promise<String> p = Promise.newPromise();
                    future.onComplete(result -> p.success(result.error().getMessage()));
                    assertEquals(errorMessage, p.future().get());
                })
        );
    }

}