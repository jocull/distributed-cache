package com.github.jocull.raftcache.lib.raft;

import java.util.concurrent.CompletableFuture;

public interface RaftOperations {
    @Deprecated
    <T> RaftLog<T> submitNewLog(T entry);

    <T> CompletableFuture<RaftLog<T>> submit(T entry);
}
