package com.codefromjames.com.lib.communication.raft;

public enum NodeState {
    NONE,
    FOLLOWER,
    CANDIDATE,
    LEADER;
}
