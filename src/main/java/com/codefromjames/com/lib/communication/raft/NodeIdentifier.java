package com.codefromjames.com.lib.communication.raft;

public class NodeIdentifier {
    private final String id;
    private final String address;
    private final String hostname;
    private NodeState state = NodeState.NONE;

    public NodeIdentifier(String id, String address, String hostname) {
        this.id = id;
        this.address = address;
        this.hostname = hostname;
    }
}
