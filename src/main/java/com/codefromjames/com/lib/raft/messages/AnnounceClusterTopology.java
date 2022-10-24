package com.codefromjames.com.lib.raft.messages;

import com.codefromjames.com.lib.topology.NodeIdentifierState;

import java.util.List;

public class AnnounceClusterTopology {
    public final List<NodeIdentifierState> nodeIdentifierStates;

    public AnnounceClusterTopology(List<NodeIdentifierState> nodeIdentifierStates) {
        this.nodeIdentifierStates = List.copyOf(nodeIdentifierStates);
    }

    public List<NodeIdentifierState> getNodeIdentifierStates() {
        return nodeIdentifierStates;
    }
}
