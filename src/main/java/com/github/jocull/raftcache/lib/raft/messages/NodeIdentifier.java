package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

public class NodeIdentifier {
    private final String id;
    private final NodeAddress nodeAddress;

    public NodeIdentifier(String id, NodeAddress nodeAddress) {
        this.id = id;
        this.nodeAddress = nodeAddress;
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    @Override
    public String toString() {
        return "NodeIdentifier{" +
                "id='" + id + '\'' +
                ", nodeAddress=" + nodeAddress +
                '}';
    }
}
