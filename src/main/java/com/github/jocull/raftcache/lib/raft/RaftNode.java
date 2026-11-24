package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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

    /**
     * Tears down a live outbound connection: it is removed from this node's active
     * connections, and the underlying channel is closed — on sockets this ends the peer side
     * of the wire link too, so the peer stops receiving from us as well. Local cleanup
     * semantics only: the node keeps operating with the remaining quorum and does NOT
     * auto-reconnect; re-establishment is an explicit {@link #connectWithTopology()} or
     * {@link #connectTo(NodeAddress)} (reconnect policy is a recorded follow-up,
     * {@code NETWORKING.md} 6.1). Idempotent: disconnecting a connection that is already gone
     * succeeds as a no-op.
     *
     * @param connection the outbound connection to close (as returned by
     *                   {@link #connectWithTopology()}/{@link #connectTo(NodeAddress)}, or a live
     *                   one such as an accepted inbound link)
     */
    CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection);

    CompletableFuture<Optional<StateResponse>> getLeader();

    CompletableFuture<List<StateResponse>> getClusterNodeStates();

    <T> CompletableFuture<RaftLog<T>> submit(T entry);

    /**
     * Supplies the portable state this node's leader behavior would install on a laggard
     * follower (SNAPSHOTS.md stage 5, Raft §7): a state-machine snapshot object the
     * receiver's state machine understands, crossing the wire as a loose {@link Object}.
     *
     * <p>Default is a no-op: {@link RaftNode} implementations that carry no state machine
     * to snapshot (test doubles, middleware fakes) simply never have their
     * {@link #snapshotSupplier() supplier} consulted. A leader whose supplier is absent
     * (or yields {@code null}) continues to recover laggards with plain {@link AppendEntries}
     * only.
     */
    default void setSnapshotSupplier(Supplier<Object> snapshotSupplier) {
        // Intentionally empty: see above.
    }

    /**
     * The snapshot supplier installed via {@link #setSnapshotSupplier(Supplier)}, or
     * {@code null} when this node carries no snapshot source. Readers treat {@code null}
     * as "no snapshots" and fall back to log appends.
     */
    default Supplier<Object> snapshotSupplier() {
        return null;
    }

    /**
     * Installs the durable store this node persists snapshots to (SNAPSHOTS.md stage 6b):
     * a leader saves its capture on compaction, a follower saves the exact wire bytes it
     * installs, and both reload any saved snapshot on startup to re-anchor state machine
     * and log before replaying the log.
     *
     * <p>Default is a no-op: {@link RaftNode} implementations that carry no durable backing
     * (test doubles, middleware fakes) simply never persist snapshots.
     */
    default void setSnapshotStore(SnapshotStore snapshotStore) {
        // Intentionally empty: see above.
    }

    /**
     * The snapshot store installed via {@link #setSnapshotStore(SnapshotStore)}, or
     * {@code null} when this node has no durable snapshot backing. Readers must treat
     * {@code null} as "no store" and take no snapshot action on compaction or install.
     */
    default SnapshotStore snapshotStore() {
        return null;
    }

    /**
     * The event bus this node publishes its events on (the same instance the
     * {@code RaftManager} that created it exposes via {@code getEventBus()}).
     *
     * <p>State-machine facades use this to pick out the node they own when wiring their
     * {@link #setSnapshotSupplier(Supplier) snapshot supplier}: a facade supplies the
     * state of exactly one node (its local state machine), never one it does not own.
     * Implementations that carry no event bus (test doubles) may keep the default,
     * which supplies no snapshots at all.
     */
    default com.github.jocull.raftcache.lib.event.EventBus getEventBus() {
        return null;
    }
}
