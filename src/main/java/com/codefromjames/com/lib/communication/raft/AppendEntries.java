package com.codefromjames.com.lib.communication.raft;

import java.util.List;

public class AppendEntries {
    // arguments
    long term; // leader's term
    String leaderId; // so follower can redirect clients
    long prevLogIndex; // index of log immediately preceding new ones
    List<Object> entries; // log entries to store (or empty for heartbeat)
    long leaderCommit; // leader's commit index

    // results (probably another class)
    long rTerm; // current term, for the leader to update itself
    boolean rSuccess; // true if follower contained entry matching prevLogIndex and prevLogTerm
}
