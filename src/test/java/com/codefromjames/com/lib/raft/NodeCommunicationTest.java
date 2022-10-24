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
    void testNodeCommunication() {
        final PassThruMiddleware middleware = new PassThruMiddleware();
        try (RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNode nodeA = new RaftNode(new NodeState("nodeA", new NodeAddress("addressA")), raftManager);
            final RaftNode nodeB = new RaftNode(new NodeState("nodeB", new NodeAddress("addressB")), raftManager);
            middleware.addNode(nodeA).addNode(nodeB);

            NodeCommunication aToB = nodeA.connectTo(nodeB.getNodeAddress());

            assertEquals(Set.of("nodeA"), nodeA.getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of("nodeB"), nodeB.getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));

            aToB.introduce();

            assertEquals(Set.of("nodeA", "nodeB"), nodeA.getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of("nodeA", "nodeB"), nodeB.getTopology().stream()
                    .map(NodeIdentifierState::getId)
                    .collect(Collectors.toSet()));
        }
    }
}