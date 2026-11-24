package com.github.jocull.raftcache.lib.raft.events;

import com.github.jocull.raftcache.lib.raft.RaftLog;

import java.util.List;
import java.util.Objects;

public class LogsCommitted {
    private final List<RaftLog<?>> logs;

    public LogsCommitted(List<RaftLog<?>> logs) {
        // The immutable copy rejects a nulled element (a shape List.of would itself
        // refuse) at construction, before any intake can dereference it
        this.logs = List.copyOf(Objects.requireNonNull(logs, "logs"));
    }

    public List<RaftLog<?>> getLogs() {
        return logs;
    }
}
