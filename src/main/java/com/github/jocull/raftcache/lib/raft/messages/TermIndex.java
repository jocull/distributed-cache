package com.github.jocull.raftcache.lib.raft.messages;

public class TermIndex {
    private final int term;
    private final long index;

    public TermIndex(int term, long index) {
        this.term = term;
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "TermIndex{" +
                "term=" + term +
                ", index=" + index +
                '}';
    }
}
