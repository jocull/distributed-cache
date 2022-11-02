package com.github.jocull.raftcache.lib.raft.events;

import com.github.jocull.raftcache.lib.raft.RaftLog;

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
