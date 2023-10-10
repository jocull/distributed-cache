package com.github.jocull.raftcache.proto.raft.communication;

public class ClusterJoinRequest {
    private String nodeId;

    public String nodeId() {
        return nodeId;
    }

    public ClusterJoinRequest setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }
}
