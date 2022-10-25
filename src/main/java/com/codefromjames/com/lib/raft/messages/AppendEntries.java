package com.codefromjames.com.lib.raft.messages;

public class AppendEntries {
    private final int term;
    private final long previousLogIndex;
    private final long leaderCommitIndex;

    // TODO: Needs more work, just a rough heartbeat right now to confirm leadership
    public AppendEntries(int term, long previousLogIndex, long leaderCommitIndex) {
        this.term = term;
        this.previousLogIndex = previousLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
    }

    public int getTerm() {
        return term;
    }

    public long getPreviousLogIndex() {
        return previousLogIndex;
    }

    public long getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    //    // arguments
//    long term; // leader's term
//    String leaderId; // so follower can redirect clients
//    long prevLogIndex; // index of log immediately preceding new ones
//    List<Object> entries; // log entries to store (or empty for heartbeat)
//    long leaderCommit; // leader's commit index
//
//    // results (probably another class)
//    long rTerm; // current term, for the leader to update itself
//    boolean rSuccess; // true if follower contained entry matching prevLogIndex and prevLogTerm
}
