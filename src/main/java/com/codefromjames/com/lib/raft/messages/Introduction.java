package com.codefromjames.com.lib.raft.messages;

import com.codefromjames.com.lib.topology.NodeAddress;

public class Introduction {
    private final String id;
    private final NodeAddress nodeAddress;
    private final long lastReceivedIndex;

    public Introduction(String id, NodeAddress nodeAddress, long lastReceivedIndex) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.lastReceivedIndex = lastReceivedIndex;
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public long getLastReceivedIndex() {
        return lastReceivedIndex;
    }
}
