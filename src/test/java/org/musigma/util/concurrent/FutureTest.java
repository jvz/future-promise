package org.musigma.util.concurrent;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.musigma.util.Failure;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class FutureTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        Future<String> resultFuture = Future.successful("test").transform(ignored -> new Failure<>(new IllegalStateException()));
        expectedException.expect(IllegalStateException.class);
        resultFuture.get();
    }
}