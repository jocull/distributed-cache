package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;

import java.util.concurrent.CompletableFuture;

public interface RaftOperations {
    AnnounceClusterTopology requestTopology();

    <T> CompletableFuture<RaftLog<T>> submit(T entry);
}
