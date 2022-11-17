package com.github.jocull.raftcache.lib.raft.events;

import com.github.jocull.raftcache.lib.raft.RaftLog;

public class AppendLog {
    private final RaftLog<?> log;

    public AppendLog(RaftLog<?> log) {
        this.log = log;
    }

    public RaftLog<?> getLog() {
        return log;
    }
}
