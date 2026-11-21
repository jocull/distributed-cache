package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.NodeConnectionOutbound;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-only {@link RaftNode} for exercising {@link SocketChannelMiddleware} in isolation.
 *
 * <p>It plays the role the real {@code RaftNodeImpl} plays on the <em>server</em> of a
 * connection: when the middleware dials in and calls
 * {@link #acceptConnection(ChannelMiddleware.ChannelSide)}, this node installs a receiver
 * sink on the side (recording every inbound message) and remembers the side so the test can
 * send back to the peer. Everything else (state, logs, election) is out of scope for a
 * transport test, so those methods are inert stubs.
 */
class FakeRaftNode implements RaftNode {
    private final String id;
    private final NodeAddress address;

    /** Every message delivered to this node's receiver sink, in delivery order. */
    private final List<Object> received = new CopyOnWriteArrayList<>();
    /** Every inbound side this node accepted, in accept order. */
    private final List<ChannelMiddleware.ChannelSide> acceptedSides = new CopyOnWriteArrayList<>();

    FakeRaftNode(String id, NodeAddress address) {
        this.id = id;
        this.address = address;
    }

    List<Object> received() {
        return List.copyOf(received);
    }

    /** Receiver sink for a channel's {@code setReceiver}; same effect as accepting a connection. */
    void record(Object message) {
        received.add(message);
    }

    ChannelMiddleware.ChannelSide lastAcceptedSide() {
        return acceptedSides.isEmpty() ? null : acceptedSides.get(acceptedSides.size() - 1);
    }

    /** Sides accepted so far (a receiver was installed on each, in accept order). */
    List<ChannelMiddleware.ChannelSide> getAcceptedSides() {
        return List.copyOf(acceptedSides);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public NodeAddress getNodeAddress() {
        return address;
    }

    @Override
    public CompletableFuture<NodeConnectionOutbound> acceptConnection(ChannelMiddleware.ChannelSide incoming) {
        // N1: installing the receiver completes the server half of the handshake, before the
        // middleware writes its Acknowledgement. Recording the side lets the test send back.
        incoming.setReceiver(received::add);
        acceptedSides.add(incoming);
        return CompletableFuture.completedFuture(null);
    }

    // --- Inert stubs: the transport tests never exercise election or log semantics. ---

    @Override
    public CompletableFuture<Void> start() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stop() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<NodeStates> getNodeState() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TermIndex> getCurrentTermIndex() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TermIndex> getCommittedTermIndex() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<NodeIdentifier>> getKnownNodes() {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public CompletableFuture<List<NodeConnectionOutbound>> connectWithTopology() {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public CompletableFuture<NodeConnectionOutbound> connectTo(NodeAddress nodeAddress) {
        throw new UnsupportedOperationException("FakeRaftNode does not dial out");
    }

    @Override
    public CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<StateResponse>> getLeader() {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<List<StateResponse>> getClusterNodeStates() {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
        return CompletableFuture.completedFuture(null);
    }
}
