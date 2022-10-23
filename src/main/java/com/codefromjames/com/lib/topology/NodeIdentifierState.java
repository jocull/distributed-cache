package com.codefromjames.com.lib.topology;

import com.codefromjames.com.lib.raft.NodeStates;

public class NodeIdentifierState {
    private final String id;
    private final String address;
    private final String hostname;
    private NodeStates state = NodeStates.NONE;

    public NodeIdentifierState(String id, String address, String hostname) {
        this.id = id;
        this.address = address;
        this.hostname = hostname;
    }
}
