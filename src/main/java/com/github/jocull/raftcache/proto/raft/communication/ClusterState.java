package com.github.jocull.raftcache.proto.raft.communication;

import java.util.List;

public class ClusterState {
    private TermIndex termIndex;
    private List<ClusterNode> nodes;

    public ClusterState() {
    }

    public TermIndex termIndex() {
        return termIndex;
    }

    public ClusterState setTermIndex(TermIndex termIndex) {
        this.termIndex = termIndex;
        return this;
    }

    public List<ClusterNode> nodes() {
        return nodes;
    }

    public ClusterState setNodes(List<ClusterNode> nodes) {
        this.nodes = nodes;
        return this;
    }

    private static class ClusterNode {
        private String nodeId;
        private NodeState state;
    }
}
