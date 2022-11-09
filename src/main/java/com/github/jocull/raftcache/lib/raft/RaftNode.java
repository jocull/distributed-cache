package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface RaftNode {
    String getId();

    NodeAddress getNodeAddress();

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    CompletableFuture<NodeStates> getNodeState();

    CompletableFuture<TermIndex> getCurrentTermIndex();

    CompletableFuture<TermIndex> getCommittedTermIndex();

    CompletableFuture<List<NodeIdentifier>> getKnownNodes();

    CompletableFuture<List<NodeConnectionOutbound>> connectWithTopology();

    CompletableFuture<NodeConnectionOutbound> connectTo(NodeAddress nodeAddress);

    CompletableFuture<NodeConnectionOutbound> acceptConnection(ChannelMiddleware.ChannelSide incoming);

    CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection);

    CompletableFuture<Optional<StateResponse>> getLeader();

    CompletableFuture<List<StateResponse>> getClusterNodeStates();

    <T> CompletableFuture<RaftLog<T>> submit(T entry);
}
