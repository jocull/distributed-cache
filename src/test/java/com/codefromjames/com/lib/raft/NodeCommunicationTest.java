package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.middleware.PassThruMiddleware;
import com.codefromjames.com.lib.topology.InMemoryTopologyDiscovery;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.NodeIdentifierState;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class NodeCommunicationTest {
    @Test
    void testNodeIntroductions() {
        final PassThruMiddleware middleware = new PassThruMiddleware();
        try (RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
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
            assertEquals(Set.of("nodeA", "nodeB"), nodeA.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of("nodeA", "nodeB"), nodeB.getClusterTopology().getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
        }
    }

    @Test
    void testElection() throws InterruptedException {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        final PassThruMiddleware middleware = new PassThruMiddleware();
        try (RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
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

            Thread.sleep(1000);

            final int leaderCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                    .filter(r -> r.getState().equals(NodeStates.LEADER))
                    .count();
            final int followerCount = (int) middleware.getAddressRaftNodeMap().values().stream()
                    .filter(r -> r.getState().equals(NodeStates.FOLLOWER))
                    .count();

            assertEquals(1, leaderCount, "Leader count not as expected!");
            assertEquals(2, followerCount, "Follower count not as expected!");
        }
    }
}
