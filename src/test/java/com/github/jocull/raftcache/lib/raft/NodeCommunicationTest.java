package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class NodeCommunicationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCommunicationTest.class);

    private static Stream<Arguments> provideIterationsLong() {
        return IntStream.range(1, 31)
                .mapToObj(Arguments::of);
    }

    private static Stream<Arguments> provideIterationsShort() {
        return IntStream.range(1, 6)
                .mapToObj(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testNodeIntroductions(int iteration) {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            middleware.addNode(nodeA).addNode(nodeB);

            assertEquals(Set.of(), nodeA.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifier::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of(), nodeB.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifier::getId)
                    .collect(Collectors.toSet()));

            nodeA.connectTo(nodeB.getNodeAddress());

            // nodeA self registers, and contacts nodeB
            assertWithinTimeout("nodeA's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeA.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifier::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
            assertWithinTimeout("nodeB's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeB.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifier::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElection(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            final RaftNode nodeC = new RaftNode("nodeC", new NodeAddress("addressC"), raftManager);
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // No node should be a leader right now
            assertFalse(NodeStates.LEADER.equals(nodeA.getState())
                    || NodeStates.LEADER.equals(nodeB.getState())
                    || NodeStates.LEADER.equals(nodeC.getState()), "No leader should have been found!");

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::connectWithTopology);
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::start);

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            final List<StateResponse> clusterNodeStates = nodeA.getOperations().getClusterNodeStates();
            assertNotNull(clusterNodeStates);
            assertEquals(3, clusterNodeStates.size());
            assertEquals(Set.of("nodeA", "nodeB", "nodeC"), clusterNodeStates.stream().map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));
            assertEquals(Stream.of("addressA", "addressB", "addressC").map(NodeAddress::new).collect(Collectors.toSet()),
                    clusterNodeStates.stream().map(x -> x.getIdentifier().getNodeAddress()).collect(Collectors.toSet()));
            assertEquals(Set.of("nodeA", "nodeB", "nodeC"), clusterNodeStates.stream().map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));
            assertEquals(nodes1.leader().getId(), clusterNodeStates.stream().filter(c -> c.getState().equals(NodeStates.LEADER)).findFirst().orElseThrow().getIdentifier().getId());
            assertEquals(nodes1.followers().stream().map(RaftNode::getId).collect(Collectors.toSet()),
                    clusterNodeStates.stream().filter(c -> c.getState().equals(NodeStates.FOLLOWER)).map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));

            final StateResponse leaderState = nodeA.getOperations().getLeader().orElseThrow();
            assertEquals(nodes1.leader().getId(), leaderState.getIdentifier().getId());
            assertEquals(nodes1.leader().getCurrentTerm(), leaderState.getTerm());
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElectionWithFailure_oneNode(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            final RaftNode nodeC = new RaftNode("nodeC", new NodeAddress("addressC"), raftManager);
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::connectWithTopology);
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::start);

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElectionWithFailure_twoNodes(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            final RaftNode nodeC = new RaftNode("nodeC", new NodeAddress("addressC"), raftManager);
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::connectWithTopology);
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::start);

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            middleware.interrupt(nodes1.followers().get(0).getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and candidates", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.candidates().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            middleware.restore(nodes1.followers().get(0).getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testLogReplication(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            final RaftNode nodeC = new RaftNode("nodeC", new NodeAddress("addressC"), raftManager);
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::connectWithTopology);
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::start);

            final ClusterNodes nodes = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes.followers().forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.getOperations().submit("hello"));
            });

            final RaftLog<String> log1 = assertDoesNotThrow(
                    () -> nodes.leader().getOperations().submit("hello").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 1");
            assertTrue(nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log1.getTermIndex())),
                    "Followers didn't get log 1");

            final RaftLog<String> log2 = assertDoesNotThrow(
                    () -> nodes.leader().getOperations().submit("hello again").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 2");
            assertTrue(nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log2.getTermIndex())),
                    "Followers didn't get log 2");

            final RaftLog<String> log3 = assertDoesNotThrow(
                    () -> nodes.leader().getOperations().submit("hello one last time").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 2");
            assertTrue(nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log3.getTermIndex())),
                    "Followers didn't get log 3");

            final List<CompletableFuture<RaftLog<Integer>>> futures = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes.leader().getOperations().submit(i))
                    .collect(Collectors.toList());

            final List<RaftLog<Integer>> results = futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            final RaftLog<Integer> logLast = results.get(futures.size() - 1);
            assertTrue(nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(logLast.getTermIndex())),
                    "Followers didn't get final log");
        }
    }

    @SuppressWarnings("BusyWait")
    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testLogReplicationDuringTermChange(int iteration) throws InterruptedException {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = new RaftNode("nodeA", new NodeAddress("addressA"), raftManager);
            final RaftNode nodeB = new RaftNode("nodeB", new NodeAddress("addressB"), raftManager);
            final RaftNode nodeC = new RaftNode("nodeC", new NodeAddress("addressC"), raftManager);
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::connectWithTopology);
            middleware.getAddressRaftNodeMap().values().forEach(RaftNode::start);

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes1.followers().forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.getOperations().submit("hello"));
            });

            final AtomicBoolean producerStopped = new AtomicBoolean();
            final List<CompletableFuture<RaftLog<Integer>>> logFutures = new ArrayList<>();
            final List<Integer> logsRejected = new ArrayList<>();
            final Set<RaftNode> seenLeaderNodes = new HashSet<>();
            final Function<Integer, CompletableFuture<RaftLog<Integer>>> fnSubmitLogFuture = (count) -> {
                final RaftNode currentLeader = getRaftLeader(middleware);
                if (currentLeader == null) {
                    LOGGER.warn("No current leader!");
                    throw new RuntimeException("No current leader @ " + count);
                }
                synchronized (seenLeaderNodes) {
                    seenLeaderNodes.add(currentLeader);
                }

                try {
                    final CompletableFuture<RaftLog<Integer>> future = currentLeader.getOperations().submit(count);
                    LOGGER.debug("Added {} to current leader {}", count, currentLeader.getId());
                    return future;
                } catch (Exception ex) {
                    synchronized (logsRejected) {
                        LOGGER.debug("Failed to add {} to current leader {}", count, currentLeader.getId(), ex);
                        logsRejected.add(count);
                        throw new RuntimeException("Rejected @ " + count, ex);
                    }
                }
            };

            final Thread producer = new Thread(() -> {
                final Executor delayedExecutor = CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS);
                try {
                    int counter = 0;
                    while (!producerStopped.get()) {
                        final int thisCount = counter++;
                        CompletableFuture<RaftLog<Integer>> result = new CompletableFuture<>();
                        CompletableFuture<RaftLog<Integer>> retryChain = fnSubmitLogFuture.apply(thisCount);
                        for (int retry = 0; retry < 10; retry++) {
                            // See explanation of retry logic
                            // https://stackoverflow.com/a/40487376/97964
                            retryChain = retryChain
                                    .thenApply(x -> (Throwable) null)
                                    .exceptionally(ex -> ex)
                                    .thenApplyAsync(ex -> fnSubmitLogFuture.apply(thisCount), delayedExecutor)
                                    .thenCompose(Function.identity());
                        }
                        retryChain.thenAccept(result::complete);
                        logFutures.add(result);
                        Thread.sleep(5);
                    }
                } catch (InterruptedException ex) {
                    LOGGER.info("Interrupted - exiting", ex);
                }
            });
            producer.setName("producer-thread");
            producer.setDaemon(true);
            producer.start();

            final int logStartPoint = assertResultWithinTimeout("Starting logs did not populate", 5, TimeUnit.SECONDS, () -> {
                synchronized (logFutures) {
                    if (logFutures.size() >= 100) {
                        return logFutures.size();
                    }
                    return null;
                }
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            // Stay interrupted for a while, expect that we collect more logs and see more nodes
            assertWithinTimeout("Did not get more logs and new leader during interrupt", 5, TimeUnit.SECONDS, () -> {
                boolean logsPassed = false;
                boolean leadersPassed = false;
                synchronized (logFutures) {
                    logsPassed = logFutures.size() >= (logStartPoint + 200);
                }
                synchronized (seenLeaderNodes) {
                    leadersPassed = seenLeaderNodes.size() >= 2;
                }
                return logsPassed && leadersPassed;
            });

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            producerStopped.set(true);
            assertWithinTimeout("Producer didn't stop!", 1, TimeUnit.SECONDS, () -> !producer.isAlive());

            RaftLog<Integer> integerRaftLog = assertDoesNotThrow(() -> logFutures.get(logFutures.size() - 1).get(5, TimeUnit.SECONDS));
            if (integerRaftLog != null) {
                LOGGER.info("Yay!");
            }

            // TODO: Validate that the complete set of logs arrived
        }
    }

    private static ClusterNodes getRaftLeaderAndFollowers(PassThruMiddleware middleware) {
        final RaftNode leader = getRaftLeader(middleware);
        final List<RaftNode> followers = getRaftFollowers(middleware);
        final List<RaftNode> candidates = getRaftCandidates(middleware);
        return new ClusterNodes() {
            @Override
            public RaftNode leader() {
                return leader;
            }

            @Override
            public List<RaftNode> candidates() {
                return candidates;
            }

            @Override
            public List<RaftNode> followers() {
                return followers;
            }
        };
    }

    private static List<RaftNode> getRaftFollowers(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                .collect(Collectors.toList());
    }

    private static List<RaftNode> getRaftCandidates(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getState().equals(NodeStates.CANDIDATE))
                .collect(Collectors.toList());
    }

    private static RaftNode getRaftLeader(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getState().equals(NodeStates.LEADER))
                .max(Comparator.comparingInt(RaftNode::getCurrentTerm))
                .orElse(null);
    }

    private interface ClusterNodes {
        RaftNode leader();

        List<RaftNode> candidates();

        List<RaftNode> followers();
    }

    private void assertWithinTimeout(String message, long wait, TimeUnit unit, Supplier<Boolean> fnAction) {
        assertResultWithinTimeout(message, wait, unit, () -> {
            if (fnAction.get()) {
                return true;
            }
            return null;
        });
    }

    private <T> T assertResultWithinTimeout(String message, long wait, TimeUnit unit, Supplier<T> fnAction) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            final T result = fnAction.get();
            if (result != null) {
                return result;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        throw new AssertionError(message);
    }
}
