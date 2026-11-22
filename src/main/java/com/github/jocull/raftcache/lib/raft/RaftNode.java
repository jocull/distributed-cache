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

    /**
     * Current Raft state of this node.
     * <p>
     * Observing a LEADER state and then immediately calling {@link #getCurrentTermIndex()}
     * on the same node yields the term/index that the node held IN THAT observation --
     * a consistent (state, term, index) triple -- even if the node converts behavior
     * (e.g. steps down) between the two calls.
     */
    CompletableFuture<NodeStates> getNodeState();

    /**
     * Current (term, log-index) position of this node.
     * <p>
     * If preceded by a {@link #getNodeState()} read of the same node that observed the
     * node as LEADER (and no term read since), returns the term/index sampled at that
     * observation so the state/term pair is consistent; otherwise returns the live
     * current position.
     */
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
