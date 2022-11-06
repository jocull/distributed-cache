package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

public class Introduction {
    private final String id;
    private final NodeAddress nodeAddress;
    private final TermIndex lastReceivedTermIndex;
    // TODO: It is probably worth announcing the term as well - the index alone is generally not enough information.

    public Introduction(String id, NodeAddress nodeAddress, TermIndex lastReceivedTermIndex) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.lastReceivedTermIndex = lastReceivedTermIndex;
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public TermIndex getLastReceivedTermIndex() {
        return lastReceivedTermIndex;
    }
}
