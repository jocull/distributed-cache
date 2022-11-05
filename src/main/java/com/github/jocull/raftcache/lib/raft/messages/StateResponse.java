package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

public class StateResponse extends NodeIdentifier {
    private final NodeStates state;
    private final int term;
    private final long currentIndex;
    private final long committedIndex;

    public StateResponse(String id, NodeAddress nodeAddress, NodeStates state, int term, long currentIndex, long committedIndex) {
        super(id, nodeAddress);
        this.state = state;
        this.term = term;
        this.currentIndex = currentIndex;
        this.committedIndex = committedIndex;
    }

    public NodeStates getState() {
        return state;
    }

    public int getTerm() {
        return term;
    }

    public long getCurrentIndex() {
        return currentIndex;
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    @Override
    public String toString() {
        return "StateResponse{" +
                "state=" + state +
                ", term=" + term +
                ", currentIndex=" + currentIndex +
                ", committedIndex=" + committedIndex +
                "} " + super.toString();
    }
}
