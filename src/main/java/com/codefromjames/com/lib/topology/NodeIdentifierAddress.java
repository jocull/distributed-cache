package com.codefromjames.com.lib.topology;

public class NodeIdentifierAddress {
    private final String id;
    private final NodeAddress nodeAddress;

    public NodeIdentifierAddress(String id, NodeAddress nodeAddress) {
        this.id = id;
        this.nodeAddress = nodeAddress;
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }
}
