package com.github.jocull.raftcache.proto.raft;

public class RaftNode {
    private final String nodeId;
    private RaftNodeBehavior behavior = new RaftNodeBehavior();

    public RaftNode(String nodeId) {
        this.nodeId = nodeId;
    }
}
