package org.musigma.util.concurrent;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.musigma.util.test.AbstractTestExecutorService;
import org.musigma.util.test.Iterators;
import org.musigma.util.Pair;
import org.musigma.util.Thunk;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class DefaultPromiseTest {

    // for verifying promise chains, their state can be either executing or resolved
    static class Chain {
        final Set<Integer> promiseIds;
        final ChainState state;

        Chain(final Set<Integer> promiseIds, final ChainState state) {
            this.promiseIds = promiseIds;
            this.state = state;
        }

        boolean isExecuting() {
            return state instanceof Executing;
        }

        boolean isResolved() {
            return state instanceof Resolved;
        }
    }

    interface ChainState {
    }

    static class Executing implements ChainState {
        final Set<Integer> handlerIds;

        Executing(final Set<Integer> handlerIds) {
            this.handlerIds = handlerIds;
        }
    }

    static class Resolved implements ChainState {
        final Thunk<Integer> result;

        Resolved(final Thunk<Integer> result) {
            this.result = result;
        }
    }

    // a handler will represent a handler id and its resolved result
    static class Handler {
        final Thunk<Integer> result;
        final int id;

        Handler(final Thunk<Integer> result, final int id) {
            this.result = result;
            this.id = id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Handler handler = (Handler) o;
            return id == handler.id &&
                    result.equals(handler.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result, id);
        }
    }

    // used for verifying behavior in tests below
    static class Tester {
        final Map<Integer, DefaultPromise<Integer>> promises = new ConcurrentHashMap<>();
        final Map<Integer, Chain> chains = new ConcurrentHashMap<>();

        private final AtomicInteger idGenerator = new AtomicInteger();

        private int newId() {
            return idGenerator.incrementAndGet();
        }

        private final ConcurrentLinkedQueue<Handler> handlerQueue = new ConcurrentLinkedQueue<>();

        private Optional<Pair<Integer, Chain>> promiseChain(final int promiseId) {
            List<Pair<Integer, Chain>> found = new ArrayList<>(1);
            for (Map.Entry<Integer, Chain> entry : chains.entrySet()) {
                for (Integer id : entry.getValue().promiseIds) {
                    if (id == promiseId) {
                        found.add(Pair.from(entry));
                    }
                }
            }
            if (found.isEmpty()) {
                return Optional.empty();
            }
            if (found.size() == 1) {
                return Optional.of(found.get(0));
            }
            throw new IllegalStateException("Promise with id " + promiseId + " found in " + found.size() + " chains");
        }

        private abstract class Assertion<T> {
            final Map<Handler, Integer> fireCounts = new ConcurrentHashMap<>();
            final Callable<T> callable;
            Thunk<T> result;

            private Assertion(final Callable<T> callable) {
                this.callable = callable;
            }

            void check() {
                assertTrue(handlerQueue.isEmpty(), "handler queue should have been cleared by previous usage");
                result = Thunk.from(callable);
                while (!handlerQueue.isEmpty()) {
                    fireCounts.compute(handlerQueue.poll(), (handler, count) -> 1 + (count == null ? 0 : count));
                }
                checkEffect();
            }

            abstract void checkEffect();
        }

        <T> void assertNoEffect(final Callable<T> callable) {
            new AssertNoEffect<>(callable).check();
        }

        private class AssertNoEffect<T> extends Assertion<T> {

            private AssertNoEffect(final Callable<T> callable) {
                super(callable);
            }

            @Override
            void checkEffect() {
                assertTrue(result.isSuccess(), "shouldn't throw exception: " + result);
                assertTrue(fireCounts.isEmpty(), "shouldn't have fired any handlers");
            }
        }

        <T> void assertHandlersFired(final Callable<T> callable, final Thunk<Integer> result, Set<Integer> handlers) {
            new AssertHandlersFired<>(callable, result, handlers).check();
        }

        private class AssertHandlersFired<T> extends Assertion<T> {
            private final Thunk<Integer> firingResult;
            private final Set<Integer> handlers;

            private AssertHandlersFired(Callable<T> callable, final Thunk<Integer> result, final Set<Integer> handlers) {
                super(callable);
                this.firingResult = result;
                this.handlers = handlers;
            }

            @Override
            void checkEffect() {
                assertTrue(result.isSuccess());
                Map<Handler, Integer> expected = handlers.stream().collect(Collectors.toMap(handlerId -> new Handler(firingResult, handlerId), ignored -> 1));
                assertEquals(expected, fireCounts);
            }
        }

        <T> void assertMaybeIllegalThrown(final Callable<T> callable) {
            new MaybeIllegalThrown<>(callable).check();
        }

        private class MaybeIllegalThrown<T> extends Assertion<T> {

            private MaybeIllegalThrown(final Callable<T> callable) {
                super(callable);
            }

            @Override
            void checkEffect() {
                if (result.isError()) {
                    assertTrue(result.error() instanceof IllegalStateException);
                }
                assertTrue(fireCounts.isEmpty());
            }
        }

        <T> void assertIllegalThrown(final Callable<T> callable) {
            new IllegalThrown<>(callable).check();
        }

        private class IllegalThrown<T> extends Assertion<T> {

            private IllegalThrown(final Callable<T> callable) {
                super(callable);
            }

            @Override
            void checkEffect() {
                assertTrue(result.isError());
                assertTrue(result.error() instanceof IllegalStateException);
                assertTrue(fireCounts.isEmpty());
            }
        }

        private void assertPromiseValues() {
            for (Map.Entry<Integer, Chain> entry : chains.entrySet()) {
                Chain chain = entry.getValue();
                for (Integer promiseId : chain.promiseIds) {
                    if (chain.isResolved()) {
                        Thunk<Integer> expected = ((Resolved) chain.state).result;
                        Thunk<Integer> actual = promises.get(promiseId).getCurrent().orElseThrow(AssertionFailedError::new);
                        assertEquals(expected, actual);
                    }
                }
            }
        }

        // creates a new promise and returns its id
        int newPromise() {
            int promiseId = newId();
            int chainId = newId();
            promises.put(promiseId, new DefaultPromise<>());
            chains.put(chainId, new Chain(Collections.singleton(promiseId), new Executing(Collections.emptySet())));
            assertPromiseValues();
            return promiseId;
        }

        // completes a promise by id
        void complete(final int promiseId) {
            Thunk<Integer> result = Thunk.value(newId());
            Callable<Promise<Integer>> completePromise = () -> promises.get(promiseId).complete(result);
            Pair<Integer, Chain> chainPair = promiseChain(promiseId).orElseThrow(AssertionFailedError::new);
            int chainId = chainPair.getLeft();
            Chain chain = chainPair.getRight();
            Chain updated;
            if (chain.isExecuting()) {
                assertHandlersFired(completePromise, result, ((Executing) chain.state).handlerIds);
                updated = new Chain(chain.promiseIds, new Resolved(result));
            } else {
                assertIllegalThrown(completePromise);
                updated = chain;
            }
            chains.put(chainId, updated);
            assertPromiseValues();
        }

        // used in link()
        private interface MergeOp {
        }

        private enum NoMerge implements MergeOp {INSTANCE}

        private static class Merge implements MergeOp {
            final ChainState state;

            private Merge(final ChainState state) {
                this.state = state;
            }
        }

        // link two promises together and return their potentially updated chain ids
        Pair<Integer, Integer> link(final int promiseIdA, final int promiseIdB) {
            DefaultPromise<Integer> promiseA = promises.get(promiseIdA);
            DefaultPromise<Integer> promiseB = promises.get(promiseIdB);
            Pair<Integer, Chain> pairA = promiseChain(promiseIdA).orElseThrow(AssertionFailedError::new);
            int chainIdA = pairA.getLeft();
            Chain chainA = pairA.getRight();
            Pair<Integer, Chain> pairB = promiseChain(promiseIdB).orElseThrow(AssertionFailedError::new);
            int chainIdB = pairB.getLeft();
            Chain chainB = pairB.getRight();

            Callable<Void> linkPromises = () -> {
                promiseA.linkRootOf(promiseB, null);
                return null;
            };
            MergeOp mergeOp;
            if (chainA.isExecuting() && chainB.isExecuting()) {
                assertNoEffect(linkPromises);
                Set<Integer> mergedHandlerIds = new HashSet<>(((Executing) chainA.state).handlerIds);
                mergedHandlerIds.addAll(((Executing) chainB.state).handlerIds);
                mergeOp = new Merge(new Executing(mergedHandlerIds));
            } else if (chainA.isExecuting() && chainB.isResolved()) {
                Thunk<Integer> result = ((Resolved) chainB.state).result;
                assertHandlersFired(linkPromises, result, ((Executing) chainA.state).handlerIds);
                mergeOp = new Merge(new Resolved(result));
            } else if (chainA.isResolved() && chainB.isExecuting()) {
                Thunk<Integer> result = ((Resolved) chainA.state).result;
                assertHandlersFired(linkPromises, result, ((Executing) chainB.state).handlerIds);
                mergeOp = new Merge(new Resolved(result));
            } else {
                mergeOp = NoMerge.INSTANCE;
                if (chainIdA == chainIdB) {
                    assertMaybeIllegalThrown(linkPromises);
                } else {
                    assertIllegalThrown(linkPromises);
                }
            }

            int newChainIdA, newChainIdB;
            if (mergeOp == NoMerge.INSTANCE) {
                newChainIdA = chainIdA;
                newChainIdB = chainIdB;
            } else {
                ChainState state = ((Merge) mergeOp).state;
                chains.remove(chainIdA);
                chains.remove(chainIdB);
                newChainIdA = newChainIdB = newId();
                Set<Integer> promiseIds = new HashSet<>(chainA.promiseIds);
                promiseIds.addAll(chainB.promiseIds);
                chains.put(newChainIdA, new Chain(promiseIds, state));
            }

            assertPromiseValues();
            return Pair.of(newChainIdA, newChainIdB);
        }

        // attaches a handler with resolved value to a promise
        int attachHandler(final int promiseId) {
            int handlerId = newId();
            DefaultPromise<Integer> promise = promises.get(promiseId);
            Pair<Integer, Chain> chainPair = promiseChain(promiseId).orElseThrow(AssertionFailedError::new);
            int chainId = chainPair.getLeft();
            Chain chain = chainPair.getRight();
            ChainState state;
            ExecutorService inline = new AbstractTestExecutorService() {
                @Override
                public void execute(final Runnable command) {
                    command.run();
                }

                @Override
                public void reportFailure(final Throwable error) {
                    error.printStackTrace();
                }
            };
            Callable<Void> attachHandler = () -> {
                promise.onComplete(result -> handlerQueue.add(new Handler(result, handlerId)), inline);
                return null;
            };
            if (chain.isExecuting()) {
                assertNoEffect(attachHandler);
                Set<Integer> handlerIds = new HashSet<>(((Executing) chain.state).handlerIds);
                handlerIds.add(handlerId);
                state = new Executing(handlerIds);
            } else {
                Thunk<Integer> result = ((Resolved) chain.state).result;
                assertHandlersFired(attachHandler, result, Collections.singleton(handlerId));
                state = new Resolved(result);
            }
            chains.put(chainId, new Chain(chain.promiseIds, state));
            assertPromiseValues();
            return handlerId;
        }
    }

    interface Action {
    }

    static class Complete implements Action {
        private final int promiseKey;

        Complete(final int promiseKey) {
            this.promiseKey = promiseKey;
        }

        @Override
        public String toString() {
            return "Complete{" + promiseKey + '}';
        }
    }

    static class Link implements Action {
        private final int promiseKeyA;
        private final int promiseKeyB;

        Link(final int promiseKeyA, final int promiseKeyB) {
            this.promiseKeyA = promiseKeyA;
            this.promiseKeyB = promiseKeyB;
        }

        @Override
        public String toString() {
            return "Link{" + promiseKeyA + "->" + promiseKeyB + '}';
        }
    }

    static class AttachHandler implements Action {
        private final int promiseKey;

        AttachHandler(final int promiseKey) {
            this.promiseKey = promiseKey;
        }

        @Override
        public String toString() {
            return "AttachHandler{" + promiseKey + '}';
        }
    }

    static class DynamicTestActionGenerator {
        private final Tester tester = new Tester();
        private final Map<Integer, Integer> promises = new ConcurrentHashMap<>();

        private DynamicTest generate(final Action action) {
            if (action instanceof Complete) {
                return dynamicTest(action.toString(), () -> {
                    int id = promises.computeIfAbsent(((Complete) action).promiseKey, key -> tester.newPromise());
                    tester.complete(id);
                });
            } else if (action instanceof Link) {
                return dynamicTest(action.toString(), () -> {
                    int idA = promises.computeIfAbsent(((Link) action).promiseKeyA, key -> tester.newPromise());
                    int idB = promises.computeIfAbsent(((Link) action).promiseKeyB, key -> tester.newPromise());
                    tester.link(idA, idB);
                });
            } else {
                return dynamicTest(action.toString(), () -> {
                    int id = promises.computeIfAbsent(((AttachHandler) action).promiseKey, key -> tester.newPromise());
                    tester.attachHandler(id);
                });
            }
        }

        DynamicContainer generate(final String displayName, final List<Action> actions) {
            return dynamicContainer(displayName, actions.stream().map(this::generate));
        }
    }

    private static Iterator<DynamicNode> generateTestPermutations(final int count) {
        List<Complete> completes = IntStream.range(0, count).mapToObj(Complete::new).collect(Collectors.toList());
        List<Link> links = IntStream.range(0, count).boxed().flatMap(a ->
                IntStream.range(0, count).mapToObj(b ->
                        new Link(a, b)))
                .collect(Collectors.toList());
        List<AttachHandler> attaches = IntStream.range(0, count).mapToObj(AttachHandler::new).collect(Collectors.toList());
        List<Action> allActions = new ArrayList<>(completes.size() + links.size() + attaches.size());
        allActions.addAll(completes);
        allActions.addAll(links);
        allActions.addAll(attaches);
        DynamicTestActionGenerator generator = new DynamicTestActionGenerator();
        AtomicInteger permutationCounter = new AtomicInteger();
        Iterator<List<Action>> permutations = Iterators.permutationsOf(allActions);
        return Iterators.map(permutations, actions -> generator.generate("Permutation" + permutationCounter.incrementAndGet(), actions));
    }

    @TestFactory
    Iterator<DynamicNode> testPermutations1() {
        return generateTestPermutations(1);
    }

    @TestFactory
    Iterator<DynamicNode> testPermutations2() {
        // NOTE: this generates over 300,000 tests! I wouldn't recommend trying a value of 3 or higher ;)
        return generateTestPermutations(2);
    }

    interface FlatMapEvent {
    }

    static class FMLink implements FlatMapEvent {
        final int promiseIdA;
        final int promiseIdB;

        FMLink(final int promiseIdA, final int promiseIdB) {
            this.promiseIdA = promiseIdA;
            this.promiseIdB = promiseIdB;
        }
    }

    static class FMComplete implements FlatMapEvent {
        final int promiseId;

        FMComplete(final int promiseId) {
            this.promiseId = promiseId;
        }
    }

    @Test
    void simulateFlatMapLinking() {
        Random random = new Random(42);
        for (int i = 0; i < 10; i++) {
            Tester tester = new Tester();
            int flatMapCount = 100;
            Deque<FlatMapEvent> events = new ArrayDeque<>(flatMapCount + 1);
            int previousPromiseId = tester.newPromise();
            for (int j = 0; j < flatMapCount; j++) {
                int nextPromiseId = tester.newPromise();
                events.addFirst(new FMLink(nextPromiseId, previousPromiseId));
                previousPromiseId = nextPromiseId;
            }
            events.addFirst(new FMComplete(previousPromiseId));

            List<FlatMapEvent> list = new ArrayList<>(events);
            assertEquals(flatMapCount + 1, tester.chains.size(), "expected all promises to be unlinked");
            Collections.shuffle(list, random);
            // randomly link and complete promises
            for (FlatMapEvent event : list) {
                if (event instanceof FMLink) {
                    FMLink link = (FMLink) event;
                    tester.link(link.promiseIdA, link.promiseIdB);
                } else {
                    tester.complete(((FMComplete) event).promiseId);
                }
            }
            assertEquals(1, tester.chains.size(), "all promises should have linked into one chain");
        }
    }

    @Test
    void testFlatMapLinking() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            int flatMapCount = 100;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(flatMapCount + 1);
            Consumer<Runnable> execute = r -> {
                final Batching.DefaultExecutorService es = (Batching.DefaultExecutorService) Executors.common();
                es.execute(() -> {
                    try {
                        startLatch.await();
                        r.run();
                        doneLatch.countDown();
                    } catch (Exception e) {
                        es.reportFailure(e);
                    }
                });
            };
            DefaultPromise<Integer> first = new DefaultPromise<>();
            DefaultPromise<Integer> previousPromise = first;
            for (int j = 0; j < flatMapCount; j++) {
                DefaultPromise<Integer> p1 = previousPromise;
                DefaultPromise<Integer> p2 = new DefaultPromise<>();
                execute.accept(() -> p2.linkRootOf(p1, null));
                previousPromise = p2;
            }
            DefaultPromise<Integer> last = previousPromise;
            execute.accept(() -> last.success(1));
            startLatch.countDown();
            doneLatch.await();
            assertEquals(Optional.of(Thunk.value(1)), first.getCurrent());
        }
    }

}