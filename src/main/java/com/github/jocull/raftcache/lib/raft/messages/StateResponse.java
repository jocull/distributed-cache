package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

import java.io.Serializable;
import java.util.List;

public class StateResponse extends Response implements Serializable {
    private static final long serialVersionUID = 1L;

    private final NodeIdentifier identifier;
    private final NodeStates state;
    private final int term;
    private final TermIndex currentTermIndex;
    private final TermIndex committedTermIndex;
    private final List<NodeAddress> cluster;

    public StateResponse(Request request, String nodeId, NodeAddress nodeAddress, NodeStates state, int term,
                         TermIndex currentTermIndex, TermIndex committedTermIndex) {
        this(request, nodeId, nodeAddress, state, term, currentTermIndex, committedTermIndex, List.of());
    }

    // `cluster` (client B §4.2, 2026-11-25): the node's current cluster view — the
    // dialable node addresses it knows. Additive to the pre-B shape: a sender that
    // does not have a view reports an empty cluster, and a client falls back to its
    // bootstrap list.
    public StateResponse(Request request, String nodeId, NodeAddress nodeAddress, NodeStates state, int term,
                         TermIndex currentTermIndex, TermIndex committedTermIndex, List<NodeAddress> cluster) {
        super(request);
        this.identifier = new NodeIdentifier(nodeId, nodeAddress);
        this.state = state;
        this.term = term;
        this.currentTermIndex = currentTermIndex;
        this.committedTermIndex = committedTermIndex;
        this.cluster = cluster == null ? List.of() : List.copyOf(cluster);
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

    public List<NodeAddress> getCluster() {
        return cluster;
    }

    @Override
    public String toString() {
        return "StateResponse{" +
                "identifier=" + identifier +
                ", state=" + state +
                ", term=" + term +
                ", currentTermIndex=" + currentTermIndex +
                ", committedTermIndex=" + committedTermIndex +
                ", cluster=" + cluster +
                "} " + super.toString();
    }
}
