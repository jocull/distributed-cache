package com.github.jocull.raftcache.lib.raft.messages;

public class AcknowledgeEntries {
    // Current term, for the leader to update itself
    private final int term;
    // True if follower contained entry matching previous log entry and term
    private final boolean success;
    // Contains the current index after acknowledging
    private final TermIndex currentTermIndex;

    public AcknowledgeEntries(int term, boolean success, TermIndex currentTermIndex) {
        this.term = term;
        this.success = success;
        this.currentTermIndex = currentTermIndex;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public TermIndex getCurrentTermIndex() {
        return currentTermIndex;
    }

    @Override
    public String toString() {
        return "AcknowledgeEntries{" +
                "term=" + term +
                ", success=" + success +
                ", currentTermIndex=" + currentTermIndex +
                '}';
    }
}
