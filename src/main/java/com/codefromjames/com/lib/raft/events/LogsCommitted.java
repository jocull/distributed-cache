package com.codefromjames.com.lib.raft.events;

import com.codefromjames.com.lib.raft.RaftLog;

import java.util.List;

public class LogsCommitted {
    private final List<RaftLog<?>> logs;

    public LogsCommitted(List<RaftLog<?>> logs) {
        this.logs = logs;
    }

    public List<RaftLog<?>> getLogs() {
        return logs;
    }
}
