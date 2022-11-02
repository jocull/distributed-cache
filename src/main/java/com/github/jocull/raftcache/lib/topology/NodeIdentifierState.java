package com.github.jocull.raftcache.lib.topology;

import com.github.jocull.raftcache.lib.raft.NodeStates;

public class NodeIdentifierState {
    private final String id;
    private final NodeAddress nodeAddress;
    private NodeStates state = NodeStates.FOLLOWER;

    public NodeIdentifierState(String id, NodeAddress nodeAddress) {
        this.id = id;
        this.nodeAddress = nodeAddress;
    }

    public NodeIdentifierState(String id, NodeAddress nodeAddress, NodeStates nodeStates) {
        this(id, nodeAddress);
        this.state = nodeStates;
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

    public NodeIdentifierState setState(NodeStates state) {
        this.state = state;
        return this;
    }
}
