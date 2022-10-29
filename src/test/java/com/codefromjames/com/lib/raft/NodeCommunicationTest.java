package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.middleware.PassThruMiddleware;
import com.codefromjames.com.lib.topology.InMemoryTopologyDiscovery;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.NodeIdentifierState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class NodeCommunicationTest {
    @Test
    void testNodeIntroductions() throws InterruptedException {
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
            assertWithinTimeout("nodeA's topology did not settle with nodeA, nodeB", 100, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeA.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifierState::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
            assertWithinTimeout("nodeB's topology did not settle with nodeA, nodeB", 100, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeB.getClusterTopology().getTopology().stream()
                        .map(NodeIdentifierState::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
        }
    }

    @Test
    void testElection() {
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

    @Test
    void testElectionWithFailure() throws InterruptedException {
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

            final LeaderAndFollowers nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final LeaderAndFollowers result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // TODO: Validate this test. The network disconnect wasn't working properly when it was written.
            final NodeCommunication connection = nodes1.leader().getActiveConnections().get(0);
            nodes1.leader().disconnect(connection);
            Thread.sleep(2000);
            nodes1.leader().connectTo(connection.getRemoteNodeAddress());

            final LeaderAndFollowers nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final LeaderAndFollowers result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });
        }
    }

    @Test
    void testLogReplication() throws InterruptedException {
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

            final LeaderAndFollowers nodes = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final LeaderAndFollowers result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes.followers().forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.submitNewLog("hello"));
            });

            final RaftLog<String> log1 = nodes.leader().submitNewLog("hello");
            assertWithinTimeout("Followers didn't get log 1", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log1.getTerm(), log1.getIndex())));

            final RaftLog<String> log2 = nodes.leader().submitNewLog("hello again");
            assertWithinTimeout("Followers didn't get log 2", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log2.getTerm(), log2.getIndex())));

            final RaftLog<String> log3 = nodes.leader().submitNewLog("hello one last time");
            assertWithinTimeout("Followers didn't get log 3", 1, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(log3.getTerm(), log3.getIndex())));

            final List<RaftLog<Integer>> logList = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes.leader().submitNewLog(i))
                    .collect(Collectors.toList());

            final RaftLog<Integer> logLast = logList.get(logList.size() - 1);
            assertWithinTimeout("Followers didn't get final log", 5, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(logLast.getTerm(), logLast.getIndex())));
        }
    }

    @Test
    void testLogReplicationDuringTermChange() throws InterruptedException {
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

            final LeaderAndFollowers nodes = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final LeaderAndFollowers result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes.followers().forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.submitNewLog("hello"));
            });

            final List<RaftLog<Integer>> logList = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes.leader().submitNewLog(i))
                    .collect(Collectors.toList());

            // TODO: Validate this test. The network disconnect wasn't working properly when it was written.
            Thread.sleep(125);
            final NodeCommunication connection = nodes.leader().getActiveConnections().get(0);
            nodes.leader().disconnect(connection);
            Thread.sleep(500);
            nodes.leader().connectTo(connection.getRemoteNodeAddress());

            final RaftLog<Integer> logLast = logList.get(logList.size() - 1);
            assertWithinTimeout("Followers didn't get final log", 5, TimeUnit.SECONDS, () ->
                    nodes.followers().stream().allMatch(r -> r.getLogs().containsStartPoint(logLast.getTerm(), logLast.getIndex())));
        }
    }

    private static LeaderAndFollowers getRaftLeaderAndFollowers(PassThruMiddleware middleware) {
        final RaftNode leader = getRaftLeader(middleware);
        final List<RaftNode> followers = getRaftFollowers(middleware);
        return new LeaderAndFollowers() {
            @Override
            public RaftNode leader() {
                return leader;
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

    private static RaftNode getRaftLeader(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getState().equals(NodeStates.LEADER))
                .findFirst()
                .orElse(null);
    }

    private interface LeaderAndFollowers {
        RaftNode leader();

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
        }
        throw new AssertionError(message);
    }
}
