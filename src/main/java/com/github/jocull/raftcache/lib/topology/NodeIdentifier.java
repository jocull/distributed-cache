package com.github.jocull.raftcache.lib.topology;

import java.io.Serializable;

public class NodeIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

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
