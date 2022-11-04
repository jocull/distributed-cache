package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.topology.NodeAddress;

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
        // TODO: In addition to state, `term` is also important.
        //       It is important because a cluster that isn't communicating well may have two different ideas
        //       about who is the leader depending on the term. Allowing clients to compare what they see
        //       gives them additional power to evaluate the network state to make the best leader decision depending
        //       on what nodes are able to respond.

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
