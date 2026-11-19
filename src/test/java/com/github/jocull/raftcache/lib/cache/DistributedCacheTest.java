package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftManager;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/*
 * End-to-end tests of the phase-4 client API over a live in-process cluster:
 * writes are routed through the leader, and the apply-then-acknowledge ordering
 * guarantees a read immediately after put sees the value (no polling here by design --
 * any regression of the ordering would surface as an immediate-miss assertion failure).
 */
public class DistributedCacheTest {
    private static Stream<Arguments> provideIterations() {
        return IntStream.range(1, 6)
                .mapToObj(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("provideIterations")
    void testPutThenGetThroughCluster(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        // Production has one bus per process (nodes, map, and cache all share it); this
        // in-process harness separates them to prove the cache wires the right bus itself
        final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final RaftNode leader = awaitLeader(middleware, "Did not get expected leader");
            assertNotNull(leader, "No leader elected");

            try (final DistributedCache cache = new DistributedCache(raftManager.getEventBus(),
                    List.of(nodeA, nodeB, nodeC), partitionMap)) {

                // Reads of a key that has never been written are empty
                assertFalse(cache.get("facade-key").isPresent(), "Key should not exist before the first write");

                final RaftLog<KeyDataInput> logFirst = assertDoesNotThrow(
                        () -> cache.put("facade-key", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS),
                        "Expected put(v1) to commit through the leader within the timeout");

                // The put future only completes after the entry was committed AND applied (apply-
                // then-acknowledge), so the value must be visible right now -- no polling. This is
                // the client-visible half of the ordering guarantee
                final KeyDataOutput afterFirst = cache.get("facade-key")
                        .orElseThrow(() -> new AssertionError("Expected facade-key to be visible immediately after put(v1) completed"));
                assertEquals("facade-key", afterFirst.getKey());
                assertEquals("v1", new String(afterFirst.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be v1");
                assertEquals(1L, afterFirst.getVersion(), "First write should be applied exactly once");
                assertTrue(cache.getLastAppliedIndex() >= logFirst.getTermIndex().getIndex(),
                        "Cache applied index " + cache.getLastAppliedIndex() + " is below the committed index "
                                + logFirst.getTermIndex().getIndex()
                                + " -- put must only be acknowledged after the entry was applied");

                final RaftLog<KeyDataInput> logSecond = assertDoesNotThrow(
                        () -> cache.put("facade-key", Map.of("value", "v2".getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS),
                        "Expected put(v2) to commit through the leader within the timeout");

                final KeyDataOutput afterSecond = cache.get("facade-key")
                        .orElseThrow(() -> new AssertionError("Expected facade-key to be visible immediately after put(v2) completed"));
                assertEquals("v2", new String(afterSecond.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be the latest write");
                assertEquals(2L, afterSecond.getVersion(), "Both writes should be applied exactly once");
                assertTrue(cache.getLastAppliedIndex() >= logSecond.getTermIndex().getIndex(),
                        "Cache applied index " + cache.getLastAppliedIndex() + " is below the committed index "
                                + logSecond.getTermIndex().getIndex());
            }
        }
    }

    // A single-node cluster is its own quorum (election, commit, apply), and the facade must
    // route the write through that node and see the result. Before the self-election/quorum
    // fixes this whole flow was impossible
    @ParameterizedTest
    @MethodSource("provideIterations")
    void testPutThenGetSingleNodeCluster(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress());
            middleware.addNode(nodeA);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final RaftNode leader = awaitLeader(middleware, "Did not get expected single-node leader");
            assertNotNull(leader, "No leader elected");
            assertEquals("nodeA", leader.getId());

            try (final DistributedCache cache = new DistributedCache(raftManager.getEventBus(),
                    List.of(nodeA), partitionMap)) {
                final RaftLog<KeyDataInput> log = assertDoesNotThrow(
                        () -> cache.put("single-node-key", Map.of("value", "lone".getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS),
                        "Expected put to commit on the single-node cluster within the timeout");

                final KeyDataOutput output = cache.get("single-node-key")
                        .orElseThrow(() -> new AssertionError("Expected single-node-key to be visible immediately after put completed"));
                assertEquals("lone", new String(output.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be the committed write");
                assertEquals(1L, output.getVersion(), "Entry should be applied exactly once");
                assertTrue(cache.getLastAppliedIndex() >= log.getTermIndex().getIndex(),
                        "Cache applied index " + cache.getLastAppliedIndex() + " is below the committed index "
                                + log.getTermIndex().getIndex());
            }
        }
    }

    @Test
    void testConstructorRejectsEmptyNodeList() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributedCache(new EventBus(), List.of(), new PartitionMap(new EventBus(), 1)),
                "The cache needs at least one node to route writes through");
    }

    private RaftNode awaitLeader(PassThruMiddleware middleware, String message) {
        return assertResultWithinTimeout(message, 5, TimeUnit.SECONDS, () -> middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> NodeStates.LEADER.equals(r.getNodeState().join()))
                .max(Comparator.comparingInt(r -> r.getCurrentTermIndex().join().getTerm()))
                .orElse(null));
    }

    private <T> T assertResultWithinTimeout(String message, long wait, TimeUnit unit, Supplier<T> fnAction) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            final T result = fnAction.get();
            if (result != null) {
                return result;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        throw new AssertionError(message);
    }
}
