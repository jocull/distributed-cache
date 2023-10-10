package com.github.jocull.raftcache.proto.raft.communication;

public enum NodeState {
    Initial,
    Follower,
    Candidate,
    Leader,
    ;
}
