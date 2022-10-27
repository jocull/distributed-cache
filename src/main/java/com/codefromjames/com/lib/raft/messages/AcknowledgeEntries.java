package com.codefromjames.com.lib.raft.messages;

public class AcknowledgeEntries {
    // Current term, for the leader to update itself
    private final int term;
    // True if follower contained entry matching previous log entry and term
    private final boolean success;
    // Contains the current index after acknowledging
    private final long currentIndex;

    public AcknowledgeEntries(int term, boolean success, long currentIndex) {
        this.term = term;
        this.success = success;
        this.currentIndex = currentIndex;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getCurrentIndex() {
        return currentIndex;
    }
}
