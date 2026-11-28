package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.AsyncUtilities;
import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.data.StateMachineBridge;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The client API for the distributed cache (phase 4: a simple, single-shard cache).
 *
 * <p>Writes are routed to the current leader. {@link RaftNode#submit(Object)} only accepts
 * writes on a leader, and the leader acknowledges the client only after the entry has been
 * committed by a majority <em>and</em> applied to the state machine (commit → apply →
 * acknowledge), so by the time {@link #put} completes the value is durable (replicated to a
 * quorum) <em>and</em> visible in this cache's {@link PartitionMap} view -- reads of that key
 * immediately afterwards are guaranteed to see it.
 *
 * <p>{@link #get} serves from the local {@link PartitionMap} view. Because the local view is
 * fed from the local node's committed log, a read can only observe data that has been
 * committed by a majority; at most it may lag behind a very recent write made by (or applied
 * by) another node, since followers apply commits asynchronously.
 *
 * <p>The cache wires its own {@link StateMachineBridge} between {@code clusterEventBus} and
 * the local {@code partitionMap}: it subscribes to the bus on which the supplied Raft nodes
 * publish {@code LogsCommitted} (the {@code RaftManager}'s bus -- not necessarily the bus a
 * {@link PartitionMap} was constructed with) and replays committed entries into the map.
 * Callers only need to supply their view of the cluster, the local state machine, and that
 * bus; no further wiring is required.
 *
 * <p>Leader routing for writes is in-process -- the supplied {@link RaftNode} instances must
 * be directly accessible (as in the in-process test cluster) -- but the same commit path is
 * also exposed over the wire for remote clients (Client B §3.4): the constructor wires the
 * node it owns with this cache's state machine via {@link RaftNode#setCacheReader(java.util.function.Function)}
 * (local-serving reads) and the leader behavior serves remote write verbs by submitting them
 * through {@link RaftNode#submit(Object)}, so remote and local writes commit and apply
 * identically.
 */
public class DistributedCache implements AutoCloseable {
    private final List<RaftNode> nodes;
    private final PartitionMap partitionMap;
    private final StateMachineBridge stateMachineBridge;
    private final RaftNode ownedNode;

    public DistributedCache(EventBus clusterEventBus, List<RaftNode> nodes, PartitionMap partitionMap) {
        Objects.requireNonNull(clusterEventBus, "clusterEventBus");
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(partitionMap, "partitionMap");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        this.nodes = List.copyOf(nodes);
        this.partitionMap = partitionMap;
        // Wire the snapshot source and REMOVE handler on the node this facade OWNS:
        final RaftNode localNode = nodes.stream()
                .filter(node -> clusterEventBus.equals(node.getEventBus()))
                .findFirst().orElse(null);
        this.ownedNode = localNode;
        if (localNode != null) {
            stateMachineBridge = new StateMachineBridge(clusterEventBus, partitionMap, localNode);
        } else {
            // No local node - can't handle removals, but existing tests may still work
            stateMachineBridge = new StateMachineBridge(clusterEventBus, partitionMap, null);
        }
        // Wire the snapshot source (SNAPSHOTS.md stage 5) on the node this facade OWNS:
        // the node living on the event bus this cache's bridge subscribes to. A facade
        // supplies the state of exactly that node's state machine -- wiring every node in
        // the list let each later-constructed facade overwrite the suppliers of every
        // node (last-wins), so a leader would have sent another node's state bound to a
        // stale (or freshly-restarted, (0,0)) applied position. Nodes the facade does not
        // own keep whatever supplier their own facade installed.
        nodes.stream()
                .filter(node -> clusterEventBus.equals(node.getEventBus()))
                .forEach(node -> {
                    node.setSnapshotSupplier(stateMachineBridge::getSnapshot);
                    // Client B §3.4 (2026-11-25): the same owned-node filter supplies the
                    // local-serving read view for remote reads over the wire — remote
                    // CacheReadRequests reaching this node are answered from THIS facade's
                    // PartitionMap, exactly like {@link #get(String)} does in-process.
                    node.setCacheReader(key -> partitionMap.getData(key).orElse(null));
                });
    }

    /**
     * Puts {@code data} under {@code key}, routed to the current leader.
     *
     * <p>The returned future completes only after the entry has been committed by a majority
     * of the cluster and applied to this cache's state machine (see apply-then-acknowledge).
     * If the cluster has no leader yet (e.g. an election is in progress) the future fails
     * with {@link IllegalStateException}; retries are the caller's responsibility.
     */
    public CompletableFuture<RaftLog<KeyDataInput>> put(String key, Map<String, byte[]> data) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        return findLeaderNode()
                .thenCompose(leader -> leader.submit(new KeyDataInput(key, data)));
    }

    /**
     * Deletes {@code key}, routed to the current leader.
     *
     * <p>The returned future completes only after the delete entry has been committed by a
     * majority of the cluster and applied to this cache's state machine (see
     * apply-then-acknowledge), so a {@link #get} immediately afterwards sees the key as absent.
     * Deleting a key that is already absent is a no-op in the state machine -- the future
     * still completes normally, so retries are safe.
     */
    public CompletableFuture<RaftLog<KeyDataDeleteInput>> delete(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return findLeaderNode()
                .thenCompose(leader -> leader.submit(new KeyDataDeleteInput(key)));
    }

    /**
     * Sets the expiration of {@code key} to "now + {@code ttl}", routed to the current leader.
     *
     * <p>The returned future completes only after the expire entry has been committed by a
     * majority of the cluster and applied to this cache's state machine (see
     * apply-then-acknowledge). "Now" is taken at apply time on each node, so expiration is
     * resolved independently per node against its local clock -- a recording of this choice,
     * acceptable for a cache.
     *
     * <p>An EXPIRE on a key that is absent (or already expired) is a no-op in the state
     * machine -- the future still completes normally, so retries are safe and the key is
     * never resurrected.
     */
    public CompletableFuture<RaftLog<KeyDataExpireInput>> expire(String key, Duration ttl) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (ttl == null) {
            throw new IllegalArgumentException("ttl must not be null");
        }
        return findLeaderNode()
                .thenCompose(leader -> leader.submit(new KeyDataExpireInput(key, ttl)));
    }

    /**
     * Returns the current value for {@code key} from the local state machine view, if present.
     */
    public Optional<KeyDataOutput> get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return partitionMap.getData(key);
    }

    /**
     * The applied-index of this cache's state machine (0 until the first apply).
     */
    public long getLastAppliedIndex() {
        return stateMachineBridge.getLastAppliedIndex();
    }

    private CompletableFuture<RaftNode> findLeaderNode() {
        final List<CompletableFuture<NodeStates>> stateFutures = nodes.stream()
                .map(RaftNode::getNodeState)
                .collect(Collectors.toList());
        return AsyncUtilities.sequence(stateFutures)
                .thenApply(states -> {
                    final List<RaftNode> leaders = new ArrayList<>(1);
                    for (int i = 0; i < nodes.size(); i++) {
                        if (NodeStates.LEADER.equals(states.get(i))) {
                            leaders.add(nodes.get(i));
                        }
                    }
                    if (leaders.isEmpty()) {
                        throw new IllegalStateException("No leader currently available in the cluster; it may be electing");
                    }
                    if (leaders.size() > 1) {
                        // Defensive: Raft terms make this nearly impossible; if it happens, we refuse
                        // to guess which leader is authoritative
                        throw new IllegalStateException("Multiple leaders reported: " + leaders.size() + " -- refusing to route a write");
                    }
                    return leaders.get(0);
                });
    }

    @Override
    public void close() {
        stateMachineBridge.close();
    }
}
