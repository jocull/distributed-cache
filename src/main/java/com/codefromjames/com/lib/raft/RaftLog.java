package com.codefromjames.com.lib.raft;

import java.util.Objects;

public class RaftLog<T> {
    private final long index;
    private final T log;

    public RaftLog(long index, T log) {
        Objects.requireNonNull(log);

        this.index = index;
        this.log = log;
    }

    public long getIndex() {
        return index;
    }

    public Class<?> getLogClass() {
        return log.getClass();
    }

    public T getLog() {
        return log;
    }

    @Override
    public String toString() {
        return "RaftLog{" +
                "index=" + index +
                ", log=" + log +
                '}';
    }
}
