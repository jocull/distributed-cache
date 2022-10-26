package com.codefromjames.com.lib.raft;

import java.util.Objects;

public class RaftLog<T> {
    private final long index;
    private final T entry;

    public RaftLog(long index, T entry) {
        Objects.requireNonNull(entry);

        this.index = index;
        this.entry = entry;
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
                "index=" + index +
                ", entry=" + entry +
                '}';
    }
}
