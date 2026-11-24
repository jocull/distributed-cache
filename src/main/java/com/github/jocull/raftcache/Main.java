package com.github.jocull.raftcache;

import com.github.jocull.raftcache.lib.cache.DistributedCache;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftManager;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Live demo of the distributed cache: brings up an N-node (default 3) cluster on loopback
 * over real sockets -- every node behind its own {@link SocketChannelMiddleware} (ephemeral
 * port), {@link RaftManager} (hence its own event bus), Raft node, {@link PartitionMap}
 * state machine, and {@link DistributedCache} facade -- then drives a small
 * put/get/expire/delete smoke against the facade and tears the cluster down in reverse
 * construction order.
 *
 * <p>Construction follows the same bootstrap discipline as the test-scope
 * {@code ClusterKit}: each middleware binds first (its bound host:port is the node's wire
 * address), each node then knows exactly its peers, and every node's
 * {@code connectWithTopology}/{@code start} runs sequentially in node order, so the two ends
 * never dial from their executors simultaneously.
 *
 * <p>Usage: plain {@code Main} for the default 3-node cluster, or {@code Main N} for an
 * N-node cluster (N &ge; 1).
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final int DEFAULT_NODE_COUNT = 3;
    private static final int PARTITION_MAP_SIZE = 4;
    private static final Duration DEMO_TTL = Duration.ofSeconds(2);
    private static final long ELECTION_BUDGET_MS = 15_000L;
    private static final long CONVERGENCE_BUDGET_MS = 10_000L;
    private static final long POLL_MS = 50L;

    // Built in node order in the build phases; torn down in reverse in shutdown(). maps and
    // caches lag the others by one phase (their phase 3), so shutdown() tolerates that.
    private final List<SocketChannelMiddleware> middlewares = new ArrayList<>();
    private final List<InMemoryTopologyDiscovery> discoveries = new ArrayList<>();
    private final List<RaftManager> managers = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<PartitionMap> maps = new ArrayList<>();
    private final List<DistributedCache> caches = new ArrayList<>();

    public static void main(String[] args) {
        new Main().run(args);
    }

    private void run(String... args) {
        try {
            final int nodeCount = nodeCountFrom(args);
            buildCluster(nodeCount);
            final RaftNode leader = awaitLeader();
            LOGGER.info("Cluster is up ({} nodes); current leader: {} (term {})",
                    nodes.size(), leader.getId(), leader.getCurrentTermIndex().join().getTerm());

            smoke();
        } finally {
            shutdown();
        }
    }

    // ---------------------------------------------------------------------------
    // Bootstrap (mirror of the ClusterKit ritual)
    // ---------------------------------------------------------------------------

    private void buildCluster(int nodeCount) {
        // Phase 1 -- per-node scaffolding, in node order. The node's wire address is this
        // middleware's bound host:port: self-skipping and the server-side handshake target
        // resolution both match on exactly this string.
        for (int i = 1; i <= nodeCount; i++) {
            final SocketChannelMiddleware middleware = new SocketChannelMiddleware();
            final InMemoryTopologyDiscovery discovery = new InMemoryTopologyDiscovery();
            final RaftManager manager = new RaftManager(discovery, middleware);
            middlewares.add(middleware);
            discoveries.add(discovery);
            managers.add(manager);
            nodes.add(manager.newNode("node" + i, middleware.getAddress()));
        }

        // Phase 2 -- topology: each node knows exactly its peers, and its own middleware
        // registry holds the node (required for the server-side handshake to resolve it).
        for (int i = 0; i < nodeCount; i++) {
            for (int j = 0; j < nodeCount; j++) {
                if (i != j) {
                    discoveries.get(i).addKnownNode(middlewares.get(j).getAddress());
                }
            }
            middlewares.get(i).addNode(nodes.get(i));
        }

        // Phase 3 -- per-node state machine + facade: each cache replays its own node's
        // committed entries into its own map -- N independent state machines, same log.
        for (int i = 0; i < nodeCount; i++) {
            maps.add(new PartitionMap(managers.get(i).getEventBus(), PARTITION_MAP_SIZE));
            caches.add(new DistributedCache(managers.get(i).getEventBus(), List.copyOf(nodes), maps.get(i)));
        }

        // Phase 4 -- bootstrap: connect, then start, sequentially per node (both ends must
        // not dial from their executors simultaneously).
        nodes.forEach(node -> node.connectWithTopology().join());
        nodes.forEach(node -> node.start().join());
        LOGGER.info("Cluster built: {}",
                nodes.stream().map(node -> node.getId() + "@" + node.getNodeAddress().getAddress())
                        .collect(Collectors.joining(", ")));
    }

    /**
     * Waits (within budget) for the cluster to settle: exactly one node in LEADER state.
     * Returns the leader; throws if no stable leader appears in time.
     */
    private RaftNode awaitLeader() {
        final long deadline = System.currentTimeMillis() + ELECTION_BUDGET_MS;
        while (System.currentTimeMillis() < deadline) {
            final List<RaftNode> leaders = nodes.stream()
                    .filter(node -> NodeStates.LEADER.equals(node.getNodeState().join()))
                    .collect(Collectors.toList());
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            sleepMillis(POLL_MS);
        }
        throw new IllegalStateException("No leader settled within " + ELECTION_BUDGET_MS + " ms; states: "
                + nodes.stream().map(node -> node.getId() + "=" + node.getNodeState().join())
                .collect(Collectors.joining(", ")));
    }

    // ---------------------------------------------------------------------------
    // Smoke: put / get / expire / delete over the wire
    // ---------------------------------------------------------------------------

    private void smoke() {
        // Any facade routes writes to the current leader; node 1's is simply the first built.
        final DistributedCache client = caches.get(0);

        // PUT -- the future completes only after the entry is committed by a majority AND
        // applied to this cache's state machine, so the read below already sees it here.
        final byte[] greeting = ("hello from a " + nodes.size() + "-node raft cluster").getBytes(StandardCharsets.UTF_8);
        client.put("greeting", Map.of("value", greeting)).join();
        awaitClusterConverged("greeting", true);
        LOGGER.info("PUT    greeting -> read back: {}",
                describe(client.get("greeting").orElseThrow().getData()));

        // EXPIRE -- committed + applied, then the key simply stops being served after its
        // deadline; every node resolves the deadline against its own clock.
        client.put("session:demo", Map.of("token", "abc123".getBytes(StandardCharsets.UTF_8))).join();
        awaitClusterConverged("session:demo", true);
        client.expire("session:demo", DEMO_TTL).join();
        LOGGER.info("EXPIRE session:demo -> ttl {} (still served now)", DEMO_TTL);
        sleepMillis(DEMO_TTL.toMillis() + 500L);
        awaitClusterConverged("session:demo", false);
        LOGGER.info("EXPIRE session:demo -> now absent on every node");

        // DELETE -- same commit+apply guarantee: absent everywhere by the time it returns.
        client.delete("greeting").join();
        awaitClusterConverged("greeting", false);
        LOGGER.info("DELETE greeting -> absent on every node");
    }

    /**
     * Waits (within budget) for every node's state machine to converge on {@code present}
     * (or its absence) for {@code key} -- the leader holds it before it acks a client, but
     * followers apply commits asynchronously, and this is the visible convergence signal.
     */
    private void awaitClusterConverged(String key, boolean present) {
        final long deadline = System.currentTimeMillis() + CONVERGENCE_BUDGET_MS;
        while (System.currentTimeMillis() < deadline) {
            if (caches.stream().allMatch(c -> c.get(key).isPresent() == present)) {
                return;
            }
            sleepMillis(POLL_MS);
        }
        throw new IllegalStateException("Cluster did not converge on "
                + (present ? "presence" : "absence") + " of '" + key + "' within " + CONVERGENCE_BUDGET_MS + " ms");
    }

    private static Map<String, String> describe(Map<String, byte[]> data) {
        return data.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue(), StandardCharsets.UTF_8)));
    }

    // ---------------------------------------------------------------------------
    // Teardown (reverse construction order)
    // ---------------------------------------------------------------------------

    private void shutdown() {
        // Per node, highest index first: facade (stop observing the bus) before its node's
        // executor dies (terminal), then the sockets (idempotent). Tolerates a partially
        // built cluster after a bootstrap failure.
        for (int i = managers.size() - 1; i >= 0; i--) {
            if (i < caches.size()) {
                closeQuietly(caches.get(i));
            }
            closeQuietly(managers.get(i)); // stops the node(s)
            closeQuietly(middlewares.get(i)); // server sockets + channels
        }
        LOGGER.info("Cluster is down");
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ex) {
            LOGGER.warn("Teardown of {} did not close cleanly", closeable, ex);
        }
    }

    // ---------------------------------------------------------------------------
    // Small utilities
    // ---------------------------------------------------------------------------

    private static int nodeCountFrom(String... args) {
        if (args.length == 0) {
            return DEFAULT_NODE_COUNT;
        }
        final int nodeCount = Integer.parseInt(args[0]);
        if (nodeCount < 1) {
            throw new IllegalArgumentException("Node count must be >= 1, got " + nodeCount);
        }
        return nodeCount;
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for cluster state");
        }
    }
}
