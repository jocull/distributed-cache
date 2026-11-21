package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.MutableClock;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftManager;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test-scope harness bringing up a production-shaped 3-node cluster over real sockets
 * (phase 3, stage C — {@code NETWORKING.md} 4/5). Every Raft node lives behind its own
 * {@link SocketChannelMiddleware} (ephemeral port on {@code 127.0.0.1}), its own
 * {@link RaftManager} (hence its own {@link com.github.jocull.raftcache.lib.event.EventBus}),
 * its own {@link PartitionMap}, and its own {@link DistributedCache} — so replication and
 * apply are genuinely per-node, unlike the in-process {@code PassThruMiddleware} harness
 * where one shared middleware routes everything and a single state machine sees it all.
 *
 * <p>Construction follows the existing bootstrap discipline: each middleware binds first
 * (its bound {@code host:port} is the node's wire address), each node then knows exactly its
 * two peers, and every node's {@code connectWithTopology}/{@code start} runs sequentially
 * in node order — the same ritual the mocked suite uses (see {@code DistributedCacheTest}).
 *
 * <p>Knobs: {@link #interrupt(int)} seals one node's middleware two-way (the N3 discipline —
 * the far side stays framed and never desynchronizes), {@link #restore(int)} lifts the
 * partition, and the node's own paths resume traffic over the already-established channels.
 * {@link #awaitLeader(String, long, TimeUnit)} / {@link #awaitNewLeader} re-assert
 * at-most-one-leader-per-term on <em>every</em> poll sample (the {@code RaftNodeTest}
 * hardening rule): a per-term split-brain fails the test on the very sample that sees it.
 */
public final class ClusterKit implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterKit.class);
    static final int NODE_COUNT = 3;
    static final int PARTITION_MAP_SIZE = 4;
    private static final long POLL_MILLIS = 50L;

    private final List<SocketChannelMiddleware> middlewares = new ArrayList<>(NODE_COUNT);
    private final List<InMemoryTopologyDiscovery> discoveries = new ArrayList<>(NODE_COUNT);
    private final List<RaftManager> managers = new ArrayList<>(NODE_COUNT);
    private final List<RaftNode> nodes = new ArrayList<>(NODE_COUNT);
    private final List<PartitionMap> maps = new ArrayList<>(NODE_COUNT);
    private final List<DistributedCache> caches = new ArrayList<>(NODE_COUNT);

    /** No shared clock: expirations resolve against {@code Clock.systemUTC()} (unused by the CRUD ITs). */
    public ClusterKit() {
        this(null);
    }

    /**
     * Builds the cluster. When {@code sharedClock} is non-null it is the clock of all three
     * {@link PartitionMap}s, so EXPIRE deadlines are cluster-wide deterministic (the
     * in-process EXPIRE tests do the same).
     *
     * @throws IllegalStateException if any connect/start join during construction fails
     *         (all resources built up to that point are then closed before rethrowing)
     */
    public ClusterKit(MutableClock sharedClock) {
        try {
            build(sharedClock);
        } catch (RuntimeException ex) {
            closeQuietly();
            throw (ex instanceof IllegalStateException) ? ex
                    : new IllegalStateException("Failed to build a 3-node socket cluster", ex);
        }
        LOGGER.info("ClusterKit up: {}",
                nodes.stream().map(node -> node.getId() + "@" + node.getNodeAddress().getAddress())
                        .collect(Collectors.joining(", ")));
    }

    private void build(MutableClock sharedClock) {
        // Phase 1 — per-node scaffolding, in node order. The node's wire address is this
        // middleware's bound host:port: self-skipping and the server-side handshake target
        // resolution both match on exactly this string.
        for (int i = 0; i < NODE_COUNT; i++) {
            final SocketChannelMiddleware middleware = new SocketChannelMiddleware();
            final InMemoryTopologyDiscovery discovery = new InMemoryTopologyDiscovery();
            final RaftManager manager = new RaftManager(discovery, middleware);
            final RaftNode node = manager.newNode("node" + (i + 1), middleware.getAddress());
            middlewares.add(middleware);
            discoveries.add(discovery);
            managers.add(manager);
            nodes.add(node);
        }

        // Phase 2 — topology: each node knows exactly its two peers, and its own middleware
        // registry holds the node (required for the server-side handshake to resolve it).
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int j = 0; j < NODE_COUNT; j++) {
                if (i != j) {
                    discoveries.get(i).addKnownNode(middlewares.get(j).getAddress());
                }
            }
            middlewares.get(i).addNode(nodes.get(i));
        }

        // Phase 3 — per-node state machine + facade. Each DistributedCache wires its own
        // bridge onto its own node's bus and replays that node's committed entries into its
        // own map -- production shape: three independent state machines, same log.
        for (int i = 0; i < NODE_COUNT; i++) {
            final PartitionMap map = sharedClock != null
                    ? new PartitionMap(managers.get(i).getEventBus(), PARTITION_MAP_SIZE, sharedClock)
                    : new PartitionMap(managers.get(i).getEventBus(), PARTITION_MAP_SIZE);
            maps.add(map);
            caches.add(new DistributedCache(managers.get(i).getEventBus(), List.copyOf(nodes), map));
        }

        // Phase 4 — bootstrap: connect, then start, sequentially per node (the PassThru
        // ritual: both ends must not dial from their executors simultaneously).
        nodes.forEach(node -> node.connectWithTopology().join());
        nodes.forEach(node -> node.start().join());
    }

    // ---------------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------------

    public int nodeCount() {
        return nodes.size();
    }

    public RaftNode node(int i) {
        return nodes.get(i);
    }

    public int indexOf(RaftNode node) {
        final int index = nodes.indexOf(node);
        if (index < 0) {
            throw new IllegalArgumentException("Not a cluster node: " + node);
        }
        return index;
    }

    public DistributedCache cache(int i) {
        return caches.get(i);
    }

    public PartitionMap map(int i) {
        return maps.get(i);
    }

    /** The local state-machine view on node {@code i} only (what its own {@code get} sees). */
    public KeyDataOutput localViewOf(int i, String key) {
        return caches.get(i).get(key)
                .orElseThrow(() -> new AssertionError("Expected key to be present on node " + i + ": " + key));
    }

    public boolean localHas(int i, String key) {
        return caches.get(i).get(key).isPresent();
    }

    // ---------------------------------------------------------------------------
    // Partition knobs (N3 discipline over real sockets)
    // ---------------------------------------------------------------------------

    /**
     * Seals node {@code i}'s middleware two-way: its outbound sends drop at the door and its
     * inbound frames are consumed but discarded, so the far side never desynchronizes. The
     * peer middlewares are untouched (a single sealing call partitions the pair).
     */
    public void interrupt(int i) {
        middlewares.get(i).interrupt(middlewares.get(i).getAddress());
    }

    /** Ends a partition started by {@link #interrupt(int)}; traffic resumes in order. */
    public void restore(int i) {
        middlewares.get(i).restore(middlewares.get(i).getAddress());
    }

    // ---------------------------------------------------------------------------
    // Election / convergence waits (at-most-one-leader-per-term asserted on every sample)
    // ---------------------------------------------------------------------------

    /**
     * Waits within the budget for the stable elected state: exactly one leader and the rest
     * followers. Every poll sample re-asserts the Raft safety invariant (at most one LEADER
     * per term) and throws on the very sample that sees a split-brain.
     */
    public RaftNode awaitLeader(String message, long wait, TimeUnit unit) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            assertAtMostOneLeaderPerTerm();
            final List<RaftNode> leaders = leaders();
            if (leaders.size() == 1 && followers().size() == NODE_COUNT - 1) {
                return leaders.get(0);
            }
            sleepMillis(POLL_MILLIS);
        }
        throw new AssertionError(message);
    }

    /**
     * Waits within the budget (post leader-partition) for exactly one leader that is not
     * {@code oldLeader} and sits at a term strictly higher than {@code oldTerm} -- votes in
     * the surviving minority can only be granted at a higher term, so this is the "new,
     * authoritative leader" of the live half. Every poll sample re-asserts
     * {@link #assertAtMostOneLeaderPerTerm()}; the old leader may still be a LEADER zombie
     * at the old term alongside it, which is exactly what this check tolerates.
     */
    public RaftNode awaitNewLeader(RaftNode oldLeader, int oldTerm, String message, long wait, TimeUnit unit) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            assertAtMostOneLeaderPerTerm();
            final List<RaftNode> fresh = leaders().stream()
                    .filter(node -> node != oldLeader && node.getCurrentTermIndex().join().getTerm() > oldTerm)
                    .collect(Collectors.toList());
            if (fresh.size() == 1) {
                return fresh.get(0);
            }
            sleepMillis(POLL_MILLIS);
        }
        throw new AssertionError(message);
    }

    /** Waits within the budget for the cluster to be back to exactly one leader (post-restore). */
    public void awaitSingleLeader(String message, long wait, TimeUnit unit) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            assertAtMostOneLeaderPerTerm();
            if (leaders().size() == 1) {
                return;
            }
            sleepMillis(POLL_MILLIS);
        }
        throw new AssertionError(message);
    }

    /**
     * Waits within the budget for node {@code i}'s state machine to have applied up to and
     * including {@code appliedIndex} (its {@code DistributedCache#getLastAppliedIndex}).
     * Followers apply commits asynchronously via {@code LogsCommitted}, so this is the
     * convergence signal for a given entry; the leader has it before it acks any client.
     */
    public void awaitApplied(int i, long appliedIndex, String message, long wait, TimeUnit unit) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            final long applied = caches.get(i).getLastAppliedIndex();
            if (applied >= appliedIndex) {
                return;
            }
            sleepMillis(POLL_MILLIS);
        }
        throw new AssertionError(message + " (node " + i + " applied " + caches.get(i).getLastAppliedIndex()
                + " < " + appliedIndex + ")");
    }

    /** {@link #awaitApplied} across the whole cluster (the leader leg is already satisfied). */
    public void awaitAppliedAll(long appliedIndex, String message, long wait, TimeUnit unit) {
        for (int i = 0; i < NODE_COUNT; i++) {
            awaitApplied(i, appliedIndex, message, wait, unit);
        }
    }

    // ---------------------------------------------------------------------------
    // Cluster state
    // ---------------------------------------------------------------------------

    public List<RaftNode> leaders() {
        return nodes.stream()
                .filter(node -> NodeStates.LEADER.equals(node.getNodeState().join()))
                .collect(Collectors.toList());
    }

    public List<RaftNode> followers() {
        return nodes.stream()
                .filter(node -> NodeStates.FOLLOWER.equals(node.getNodeState().join()))
                .collect(Collectors.toList());
    }

    /**
     * Raft safety (the election-safety guarantee): at most one node may be in LEADER state
     * within any single term. Two leaders across different terms is legal (a partitioned
     * old leader not yet hearing the higher term); two leaders at one term is never legal --
     * a hard throw fails the test on the very sample that sees it. (The {@code RaftNodeTest}
     * hardening rule, re-used here per {@code NETWORKING.md} 4.)
     */
    public void assertAtMostOneLeaderPerTerm() {
        final Map<Integer, List<String>> leadersByTerm = new TreeMap<>();
        for (final RaftNode node : nodes) {
            if (NodeStates.LEADER.equals(node.getNodeState().join())) {
                leadersByTerm.computeIfAbsent(node.getCurrentTermIndex().join().getTerm(), term -> new ArrayList<>())
                        .add(node.getId());
            }
        }
        leadersByTerm.forEach((term, nodeIds) -> {
            if (nodeIds.size() > 1) {
                throw new AssertionError("Split brain: " + nodeIds.size() + " nodes are LEADER at term " + term
                        + " (" + nodeIds + ") -- at most one leader is allowed per term");
            }
        });
    }

    // ---------------------------------------------------------------------------
    // Cleanup
    // ---------------------------------------------------------------------------

    @Override
    public void close() {
        // Reverse construction order: facades (stop observing) first, then the nodes'
        // executors, then the sockets -- per node, highest index first.
        for (int i = NODE_COUNT - 1; i >= 0; i--) {
            if (i < caches.size()) {
                closeQuietly(caches.get(i));
            }
            if (i < managers.size()) {
                closeQuietly(managers.get(i)); // stops the node(s); RaftManager does not join
            }
            if (i < middlewares.size()) {
                closeQuietly(middlewares.get(i)); // idempotent: server socket + channels
            }
        }
        LOGGER.info("ClusterKit closed");
    }

    private void closeQuietly() {
        close();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ex) {
            LOGGER.debug("ClusterKit teardown of {} did not close cleanly", closeable, ex);
        }
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for cluster state", ex);
        }
    }
}
