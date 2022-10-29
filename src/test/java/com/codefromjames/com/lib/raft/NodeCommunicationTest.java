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

            // Let the "network" settle
            Thread.sleep(100);

            // nodeA self registers, and contacts nodeB
            assertEquals(Set.of("nodeA", "nodeB"), nodeA.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of("nodeA", "nodeB"), nodeB.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
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
                final RaftNode raftLeader = middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.LEADER))
                        .findFirst()
                        .orElse(null);
                final List<RaftNode> raftFollowers = middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                        .collect(Collectors.toList());
                if (raftLeader != null && raftFollowers.size() == 2) {
                    return new LeaderAndFollowers() {
                        @Override
                        public RaftNode leader() {
                            return raftLeader;
                        }

                        @Override
                        public List<RaftNode> followers() {
                            return raftFollowers;
                        }
                    };
                }
                return null;
            });

            nodes.followers().forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.submitNewLog("hello"));
            });

            final RaftLog<String> leaderLog1 = nodes.leader().submitNewLog("hello");

            Thread.sleep(500);
            nodes.leader().submitNewLog("hello again");
            Thread.sleep(500);
            nodes.leader().submitNewLog("hello one last time");

            Thread.sleep(1000);
        }
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
