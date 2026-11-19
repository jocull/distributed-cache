package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.AsyncUtilities;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.data.StateMachineBridge;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftNode;

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
 * <p>Caveat: leader routing is in-process -- the supplied {@link RaftNode} instances must be
 * directly accessible (as in the in-process test cluster). Once real network transport lands
 * (phase 3), writes will be routed over a connection to the leader instead.
 */
public class DistributedCache implements AutoCloseable {
    private final List<RaftNode> nodes;
    private final PartitionMap partitionMap;
    private final StateMachineBridge stateMachineBridge;

    public DistributedCache(EventBus clusterEventBus, List<RaftNode> nodes, PartitionMap partitionMap) {
        Objects.requireNonNull(clusterEventBus);
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(partitionMap);
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        this.nodes = List.copyOf(nodes);
        this.partitionMap = partitionMap;
        this.stateMachineBridge = new StateMachineBridge(clusterEventBus, partitionMap);
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
        Objects.requireNonNull(key);
        Objects.requireNonNull(data);
        return findLeaderNode()
                .thenCompose(leader -> leader.submit(new KeyDataInput(key, data)));
    }

    /**
     * Returns the current value for {@code key} from the local state machine view, if present.
     */
    public Optional<KeyDataOutput> get(String key) {
        Objects.requireNonNull(key);
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
