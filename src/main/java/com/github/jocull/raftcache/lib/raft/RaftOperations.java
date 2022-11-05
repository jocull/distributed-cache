package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.StateResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface RaftOperations {
    Optional<StateResponse> getLeader();

    List<StateResponse> getClusterNodeStates();

    <T> CompletableFuture<RaftLog<T>> submit(T entry);
}
