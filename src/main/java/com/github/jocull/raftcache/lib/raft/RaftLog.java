package com.github.jocull.raftcache.lib.raft;

import java.util.Objects;

public class RaftLog<T> {
    private final TermIndex termIndex;
    private final T entry;

    public RaftLog(TermIndex termIndex, T entry) {
        Objects.requireNonNull(termIndex);
        Objects.requireNonNull(entry);

        this.termIndex = termIndex;
        this.entry = entry;
    }

    public TermIndex getTermIndex() {
        return termIndex;
    }

    public Class<?> getLogClass() {
        return entry.getClass();
    }

    public T getEntry() {
        return entry;
    }

    @Override
    public String toString() {
        return "RaftLog{" +
                "index=" + termIndex +
                ", entry=" + entry +
                '}';
    }
}
