package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.middleware.PassThruMiddleware;
import com.codefromjames.com.lib.topology.InMemoryTopologyDiscovery;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.NodeIdentifierState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
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

            int leaderCount = 0;
            int followerCount = 0;
            final long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                leaderCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.LEADER))
                        .count();
                followerCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                        .count();
                if (leaderCount == 1 && followerCount == 2) {
                    break;
                }
            }

            assertEquals(1, leaderCount, "Leader count not as expected!");
            assertEquals(2, followerCount, "Follower count not as expected!");
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

            RaftNode raftLeader = null;
            List<RaftNode> raftFollowers = null;
            final long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                raftLeader = middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.LEADER))
                        .findFirst()
                        .orElse(null);
                raftFollowers = middleware.getAddressRaftNodeMap().values().stream()
                        .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                        .collect(Collectors.toList());
                if (raftLeader != null && raftFollowers.size() == 2) {
                    break;
                }
            }
            assertNotNull(raftLeader, "Leader count not as expected!");
            assertEquals(2, raftFollowers.size(), "Follower count not as expected!");

            raftFollowers.forEach(r -> {
                assertThrows(IllegalStateException.class, () -> r.submitNewLog("hello"));
            });

            final RaftLog<String> leaderLog1 = raftLeader.submitNewLog("hello");

            Thread.sleep(500);
            raftLeader.submitNewLog("hello again");
            Thread.sleep(500);
            raftLeader.submitNewLog("hello one last time");

            Thread.sleep(1000);
        }
    }
}
