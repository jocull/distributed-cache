package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;

public class TermIndex implements Serializable {
    private static final long serialVersionUID = 1L;

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
