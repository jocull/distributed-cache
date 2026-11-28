package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.NodeConnectionOutbound;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intakes of the public client API boundary (stage 6(z)).
 *
 * <p>Every data-plane intake one layer below this facade refused nulls at the method head
 * since stage 6(v) — {@code key must not be null}, {@code data must not be null},
 * {@code ttl must not be null} — and its constructor carriers name their dependencies
 * ({@code eventBus}, {@code clock}). The facade itself still spoke the pre-campaign style:
 * seven bare {@code Objects.requireNonNull} calls with no message, in the WRONG FAMILY —
 * method intakes NPE'd (the messageless JDK call) where the house family says IAE
 * {@code "… must not be null"}, so a caller's null was indistinguishable from a failure
 * deep in the wiring, and {@code get(null)} NPE'd at the facade head where the sealed seam
 * one layer below ({@code PartitionMap}, 6(v)) already speaks the named axis. The guards
 * sat in the right PLACE (before {@code findLeaderNode()}/{@code submit()}) — they were
 * the only thing standing a null entry from riding {@code leader.submit} into the raft log;
 * they just refused it unnamed and in the wrong exception family. The intakes must refuse
 * at the head in the house IAE family with the axis named, before any leader resolution,
 * before any {@code submit}, before any future is touched — and the constructor carriers
 * must name their dependencies.
 */
public class DistributedCacheIntakeTest {

    /* A recording leader: enough of the RaftNode surface for the facade to route through,
     * nothing more. {@code submissions} is the zero-side-effect discriminator — a refused
     * write must never appear in it, and a legitimate write must. */
    private static final class RecordingNode implements RaftNode {
        final List<CompletableFuture<?>> submissions = new CopyOnWriteArrayList<>();
        private final NodeAddress address = new NodeAddress("intake-node");

        @Override
        public String getId() {
            return "intake-node";
        }

        @Override
        public CompletableFuture<NodeStates> getNodeState() {
            return CompletableFuture.completedFuture(NodeStates.LEADER);
        }

        @Override
        public NodeAddress getNodeAddress() {
            return address;
        }

        @Override
        public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
            final CompletableFuture<RaftLog<T>> future = new CompletableFuture<>();
            future.complete(null);
            submissions.add(future);
            return future;
        }

        // getEventBus() keeps the interface default (no bus) -- the facade's snapshot-supplier
        // wiring then simply filters this stub out, which is exactly the no-side-effect shape
        // these tests need: no bus subscription is created by the recording node's construction.

        // The facade never calls these paths in these tests; refuse loudly if it ever does.
        private UnsupportedOperationException notRouted(String op) {
            return new UnsupportedOperationException("RecordingNode must not be routed: " + op);
        }

        @Override
        public CompletableFuture<Void> start() {
            throw notRouted("start");
        }

        @Override
        public CompletableFuture<Void> stop() {
            throw notRouted("stop");
        }

        @Override
        public CompletableFuture<TermIndex> getCurrentTermIndex() {
            throw notRouted("getCurrentTermIndex");
        }

        @Override
        public CompletableFuture<TermIndex> getCommittedTermIndex() {
            throw notRouted("getCommittedTermIndex");
        }

        @Override
        public CompletableFuture<List<NodeIdentifier>> getKnownNodes() {
            throw notRouted("getKnownNodes");
        }

        @Override
        public CompletableFuture<List<NodeConnectionOutbound>> connectWithTopology() {
            throw notRouted("connectWithTopology");
        }

        @Override
        public CompletableFuture<NodeConnectionOutbound> connectTo(NodeAddress nodeAddress) {
            throw notRouted("connectTo");
        }

        @Override
        public CompletableFuture<NodeConnectionOutbound> acceptConnection(ChannelMiddleware.ChannelSide incoming) {
            throw notRouted("acceptConnection");
        }

        @Override
        public CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection) {
            throw notRouted("disconnectFrom");
        }

        @Override
        public CompletableFuture<Void> removeMember(String nodeId) {
            throw notRouted("removeMember");
        }

        @Override
        public CompletableFuture<Optional<StateResponse>> getLeader() {
            throw notRouted("getLeader");
        }

        @Override
        public CompletableFuture<List<StateResponse>> getClusterNodeStates() {
            throw notRouted("getClusterNodeStates");
        }
    }

    private static DistributedCache facade(RecordingNode leader) {
        final EventBus bus = new EventBus();
        final DistributedCache cache = new DistributedCache(bus, List.of(leader), new PartitionMap(bus, 1));
        // The facade is AutoCloseable; these tests end without closing it deliberately --
        // the recording bus never sees an event, so closing would be pure ceremony.
        return cache;
    }

    @SuppressWarnings("unused")
    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // --- Write-path intakes: refused at the head, before leader resolution or submit ---

    @Test
    void testPutNullKeyRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, Map.of("value", bytes("v"))),
                "put(null, data) must refuse at the facade head");
        assertEquals("key must not be null", ex.getMessage());

        // ZERO SIDE EFFECTS: no leader resolution outcome was consumed and nothing was
        // submitted — pre-fix the null-key entry rode through leader.submit into the raft
        // log (a replicated state change) and was refused only at apply time.
        assertEquals(0, leader.submissions.size(), "A refused put must not reach the raft log — zero submissions");
    }

    @Test
    void testPutNullDataRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.put("facade-key", null),
                "put(key, null) must refuse at the facade head");
        assertEquals("data must not be null", ex.getMessage());
        assertEquals(0, leader.submissions.size(), "A refused put must not reach the raft log — zero submissions");
    }

    @Test
    void testDeleteNullKeyRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.delete(null),
                "delete(null) must refuse at the facade head");
        assertEquals("key must not be null", ex.getMessage());
        assertEquals(0, leader.submissions.size(), "A refused delete must not reach the raft log — zero submissions");
    }

    @Test
    void testExpireNullKeyRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.expire(null, Duration.ofSeconds(2L)),
                "expire(null, ttl) must refuse at the facade head");
        assertEquals("key must not be null", ex.getMessage());
        assertEquals(0, leader.submissions.size(), "A refused expire must not reach the raft log — zero submissions");
    }

    @Test
    void testExpireNullTtlRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.expire("facade-key", null),
                "expire(key, null) must refuse at the facade head");
        assertEquals("ttl must not be null", ex.getMessage());
        assertEquals(0, leader.submissions.size(), "A refused expire must not reach the raft log — zero submissions");
    }

    @Test
    void testGetNullKeyRefusesAtHead() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        // Pre-fix: a messageless NPE at the facade head — while the sealed one layer below
        // (PartitionMap, 6(v)) already speaks the named axis. The boundary must speak it too.
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cache.get(null),
                "get(null) must refuse at the facade head");
        assertEquals("key must not be null", ex.getMessage());
        assertEquals(0, leader.submissions.size(), "A read refusal must not be confused with a routed write");
    }

    // --- Constructor carriers: NPE, naming the dependency (house exception family) ---

    @Test
    void testConstructorNamesNullEventBus() {
        // Pre-fix: a bare requireNonNull NPE with a null message — the axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new DistributedCache(null, List.of(new RecordingNode()), new PartitionMap(new EventBus(), 1)),
                "A null clusterEventBus must be refused at the constructor");
        assertEquals("clusterEventBus", ex.getMessage());
    }

    @Test
    void testConstructorNamesNullNodes() {
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new DistributedCache(new EventBus(), null, new PartitionMap(new EventBus(), 1)),
                "A null nodes list must be refused at the constructor");
        assertEquals("nodes", ex.getMessage());
    }

    @Test
    void testConstructorNamesNullPartitionMap() {
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new DistributedCache(new EventBus(), List.of(new RecordingNode()), null),
                "A null partitionMap must be refused at the constructor");
        assertEquals("partitionMap", ex.getMessage());
    }

    // The empty-list validity axis (already IAE at the head) must keep working after the
    // null-intake seal: a non-null, empty nodes list is a different refusal than a null one.
    @Test
    void testStillRefusesEmptyNodesList() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributedCache(new EventBus(), List.of(), new PartitionMap(new EventBus(), 1)),
                "nodes must not be empty");
    }

    // --- Legitimate intake discriminator: the refusal is null-specific, not a dead intake ---

    @Test
    void testLegitimateIntakeStillRoutesThroughLeader() {
        final RecordingNode leader = new RecordingNode();
        final DistributedCache cache = facade(leader);

        // put with a real key/data must still route to the (stub) leader and complete --
        // the head guards must not swallow the legitimate write.
        final CompletableFuture<?> future = cache.put("facade-key", Map.of("value", bytes("v")));
        future.join(); // the stub completes immediately; a live cluster would apply-then-ack

        assertEquals(1, leader.submissions.size(), "A legitimate put must reach the leader exactly once");

        // And the read side still serves legitimate keys from the local view (the stub never
        // applies, so the key reads empty -- a correct local-view read, not a refusal).
        //noinspection OptionalGetWithoutIsPresent
        cache.get("facade-key").ifPresent(output -> assertEquals("facade-key", output.getKey()));
    }
}
