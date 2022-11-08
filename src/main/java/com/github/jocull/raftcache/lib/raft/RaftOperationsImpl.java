package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class RaftOperationsImpl implements RaftOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftOperationsImpl.class);
    private final RaftNode self;

    public RaftOperationsImpl(RaftNode self) {
        this.self = self;
    }

    @Override
    public Optional<StateResponse> getLeader() {
        return getClusterNodeStates().stream()
                .filter(x -> x.getState().equals(NodeStates.LEADER))
                .max(Comparator.comparingInt(StateResponse::getTerm));
    }

    @Override
    public List<StateResponse> getClusterNodeStates() {
        throw new UnsupportedOperationException("Needs refactor");

//        final List<CompletableFuture<StateResponse>> futures;
//        synchronized (self.getActiveConnections()) {
//            futures = self.getActiveConnections().stream()
//                    .map(NodeConnectionOutbound::requestState)
//                    .collect(Collectors.toList());
//        }
//
//        final List<StateResponse> results = futures.stream()
//                .map(f -> {
//                    try {
//                        return f.join();
//                    } catch (Exception ex) {
//                        LOGGER.error("{} State response failed", self.getId(), ex);
//                        return null;
//                    }
//                })
//                .filter(Objects::nonNull)
//                .collect(Collectors.toCollection(ArrayList::new));
//
//        // Add self
//        synchronized (self) {
//            results.add(self.behavior.onStateRequest(new StateRequest()));
//        }
//
//        return results;
    }

    @Override
    public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
        if (self.getState() != NodeStates.LEADER) {
            throw new IllegalStateException("Not currently a leader! Instead " + self.getState());
        }

        return self.getLogs().appendFutureLog(self.getCurrentTerm(), entry);
    }
}
