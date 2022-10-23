package com.codefromjames.com.lib.raft;

public enum NodeStates {
    NONE,
    FOLLOWER,
    CANDIDATE,
    LEADER;
}
