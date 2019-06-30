package org.musigma.util.concurrent;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class PromiseTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void emptyPromiseShouldNotBeDone() {
        Promise<Void> p = Promise.newPromise();
        assertFalse(p.future().isDone());
        assertFalse(p.isDone());
    }

    @Test
    public void emptyPromiseShouldHaveEmptyCurrentValue() {
        Promise<Void> p = Promise.newPromise();
        assertFalse(p.future().getCurrent().isPresent());
        assertFalse(p.isDone());
    }

    @Test
    public void emptyPromiseShouldReturnSuppliedValueOnTimeout() throws ExecutionException, InterruptedException {
        Future<String> failure = Future.failed(new RuntimeException("failure"));
        Future<String> error = Future.failed(new RuntimeException("error"));
        Future<String> empty = Promise.<String>newPromise().future();
        Future<String> timeout = Future.successful("timeout");

        assertEquals("timeout", failure.fallbackTo(timeout).get());
        assertEquals("timeout", timeout.fallbackTo(empty).get());
        assertEquals("timeout", error.fallbackTo(failure).fallbackTo(timeout).get());
        expectedException.expectMessage("error");
        failure.fallbackTo(error).get();
    }

    @Test
    public void emptyPromiseShouldBeCompletableWithCompletedPromise() throws ExecutionException, InterruptedException {
        Promise<String> p1 = Promise.newPromise();
        p1.completeWith(Future.successful("foo"));
        assertEquals("foo", p1.future().get());
        Promise<String> p2 = Promise.newPromise();
        p2.completeWith(Future.failed(new RuntimeException("failed")));
        expectedException.expectMessage("failed");
        p2.future().get();
    }

}