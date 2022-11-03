package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifierState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of(), nodeB.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));

            nodeA.connectTo(nodeB.getNodeAddress());

            // nodeA self registers, and contacts nodeB
            assertWithinTimeout("nodeA's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeA.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifierState::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
            assertWithinTimeout("nodeB's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeB.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifierState::getId)
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

            assertWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final long leaderCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.LEADER))
                        .count();
                final long followerCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                        .count();
                return leaderCount == 1 && followerCount == 2;
            });
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
                assertThrows(IllegalStateException.class, () -> r.getOperations().submitNewLog("hello"));
            });

            final RaftLog<String> log1 = nodes.leader().getOperations().submitNewLog("hello");
            assertWithinTimeout("Followers didn't get log 1", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log1.getTerm(), log1.getIndex())));

            final RaftLog<String> log2 = nodes.leader().getOperations().submitNewLog("hello again");
            assertWithinTimeout("Followers didn't get log 2", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log2.getTerm(), log2.getIndex())));

            final RaftLog<String> log3 = nodes.leader().getOperations().submitNewLog("hello one last time");
            assertWithinTimeout("Followers didn't get log 3", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log3.getTerm(), log3.getIndex())));

            final List<RaftLog<Integer>> logList = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes.leader().getOperations().submitNewLog(i))
                    .collect(Collectors.toList());

            final RaftLog<Integer> logLast = logList.get(logList.size() - 1);
            assertWithinTimeout("Followers didn't get final log", 5, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(logLast.getTerm(), logLast.getIndex())));
        }
    }

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
                assertThrows(IllegalStateException.class, () -> r.getOperations().submitNewLog("hello"));
            });

            final List<RaftLog<Integer>> logList = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes1.leader().getOperations().submitNewLog(i))
                    .collect(Collectors.toList());

            Thread.sleep(125);
            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            // Stay interrupted for a while
            Thread.sleep(2000);

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            final RaftLog<Integer> logLast = logList.get(logList.size() - 1);
            assertWithinTimeout("Followers didn't get final log", 5, TimeUnit.SECONDS, () ->
                    nodes1.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(logLast.getTerm(), logLast.getIndex())));
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
                .findFirst()
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
