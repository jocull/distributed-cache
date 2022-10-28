package com.codefromjames.com.lib.raft;

import java.util.Objects;

public class RaftLog<T> {
    private final int term;
    private final long index;
    private final T entry;

    public RaftLog(int term, long index, T entry) {
        Objects.requireNonNull(entry);

        this.term = term;
        this.index = index;
        this.entry = entry;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
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
                "term=" + term +
                ", index=" + index +
                ", entry=" + entry +
                '}';
    }
}
