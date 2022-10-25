package com.codefromjames.com.lib.raft.messages;

import com.codefromjames.com.lib.raft.NodeStates;
import com.codefromjames.com.lib.topology.NodeAddress;

import java.util.List;

public class AnnounceClusterTopology {
    public final List<NodeIdentifierState> nodeIdentifierStates;

    public AnnounceClusterTopology(List<NodeIdentifierState> nodeIdentifierStates) {
        this.nodeIdentifierStates = List.copyOf(nodeIdentifierStates);
    }

    public List<NodeIdentifierState> getNodeIdentifierStates() {
        return nodeIdentifierStates;
    }

    public static class NodeIdentifierState {
        private final String id;
        private final NodeAddress nodeAddress;
        private final NodeStates state;

        public NodeIdentifierState(String id, NodeAddress nodeAddress, NodeStates state) {
            this.id = id;
            this.nodeAddress = nodeAddress;
            this.state = state;
        }

        public String getId() {
            return id;
        }

        public NodeAddress getNodeAddress() {
            return nodeAddress;
        }

        public NodeStates getState() {
            return state;
        }
    }
}
