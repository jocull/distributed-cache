package com.github.jocull.raftcache.lib.raft;

public interface RaftOperations {
    <T> RaftLog<T> submitNewLog(T entry);
}
