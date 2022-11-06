package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

public class StateResponse extends Response {
    private final NodeIdentifier identifier;
    private final NodeStates state;
    private final int term;
    private final TermIndex currentTermIndex;
    private final TermIndex committedTermIndex;

    public StateResponse(Request request, String nodeId, NodeAddress nodeAddress, NodeStates state, int term, TermIndex currentTermIndex, TermIndex committedTermIndex) {
        super(request);
        this.identifier = new NodeIdentifier(nodeId, nodeAddress);
        this.state = state;
        this.term = term;
        this.currentTermIndex = currentTermIndex;
        this.committedTermIndex = committedTermIndex;
    }

    public NodeIdentifier getIdentifier() {
        return identifier;
    }

    public NodeStates getState() {
        return state;
    }

    public int getTerm() {
        return term;
    }

    public TermIndex getCurrentTermIndex() {
        return currentTermIndex;
    }

    public TermIndex getCommittedTermIndex() {
        return committedTermIndex;
    }

    @Override
    public String toString() {
        return "StateResponse{" +
                "identifier=" + identifier +
                ", state=" + state +
                ", term=" + term +
                ", currentTermIndex=" + currentTermIndex +
                ", committedTermIndex=" + committedTermIndex +
                "} " + super.toString();
    }
}
