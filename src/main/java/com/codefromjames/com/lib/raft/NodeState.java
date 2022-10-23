package com.codefromjames.com.lib.raft;

import java.util.ArrayList;
import java.util.List;

public class NodeState {
    private final String id;
    private NodeStates state = NodeStates.FOLLOWER; // All nodes start in FOLLOWER state until they hear from a leader or start an election

    private long commitIndex = 0;
    private long lastReceivedIndex = 0;
    private List<Object> logs = new ArrayList<>();

    // TODO: Needs to link to cluster topology somehow to be aware of its surroundings?
    public NodeState(String nodeId) {
        this.id = nodeId;
    }

    // - Candidates request votes from other nodes (voting for itself)
    // - Nodes reply with their vote (based on if they have better data)
    // - The candidate becomes leader if it gets votes from a majority of nodes
}
