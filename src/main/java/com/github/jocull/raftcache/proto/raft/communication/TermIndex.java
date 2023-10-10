package com.github.jocull.raftcache.proto.raft.communication;

import java.util.Objects;

public class TermIndex {
    private final int term;
    private final long index;

    public TermIndex(int term, long index) {
        this.term = term;
        this.index = index;
    }

    public int term() {
        return term;
    }

    public long index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermIndex termIndex = (TermIndex) o;
        return term == termIndex.term && index == termIndex.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index);
    }
}
