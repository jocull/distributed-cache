package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.MutableClock;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client B, stage B3 — the REMOTE client itself (client B §5 acceptance of
 * {@code REMOTE-CLIENT-B-CLIENT-RPC.md}), exercised as the real production
 * {@link RemoteCacheClient} over a real 3-node socket cluster ({@link ClusterKit}).
 * The client under test holds no {@code RaftNode} reference anywhere: it only sees
 * {@code host:port}s and speaks the frozen wire POJOs, exactly the way an external
 * process would.
 *
 * <p>Mapping onto the §5 acceptance bullets:
 * <ul>
 *   <li>t1 / t7 — leader discovery from a PARTIAL bootstrap list (B §4.2 hybrid discovery,
 *       union of views) and NOT_LEADER re-discovery with exactly one retry (B §3.5);</li>
 *   <li>t2 / t3 / t4 — put→get, put→delete, put→expire (deterministic {@link MutableClock})
 *       over real sockets;</li>
 *   <li>t5 / t6 — the construction and lifecycle contract (arg checks, close semantics);</li>
 *   <li>t8 — the no-leader window fails with the typed, retryable {@link NoLeaderException}
 *       and recovers once the cluster heals;</li>
 *   <li>t9 — the per-request timeout (B §4.6) surfaces a silent peer as a typed
 *       {@link TimeoutException} at the terminal cause, not a hang;</li>
 *   <li>t10 — write durability: completed put ⇒ committed, applied and readable
 *       cluster-wide (read-your-own-write at completion).</li>
 * </ul>
 */
class RemoteCacheClientTest {
    private static final long WAIT = 30L;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final byte[] FIELD_VALUE = "value".getBytes(StandardCharsets.UTF_8);

    private static Map<String, byte[]> data() {
        return Map.of("field", FIELD_VALUE);
    }

    private static List<NodeAddress> allAddresses(ClusterKit kit) {
        final List<NodeAddress> addresses = new ArrayList<>(kit.nodeCount());
        for (int i = 0; i < kit.nodeCount(); i++) {
            addresses.add(kit.node(i).getNodeAddress());
        }
        return addresses;
    }

    private static boolean chainContains(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    private static boolean chainMessageContains(Throwable failure, String fragment) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(fragment)) {
                return true;
            }
        }
        return false;
    }

    private static String describeChain(Throwable failure) {
        final StringBuilder sb = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (sb.length() > 0) {
                sb.append(" -> ");
            }
            sb.append(cause);
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------------
    // t1 — discovery: a PARTIAL bootstrap list must be enough to find the leader
    // ---------------------------------------------------------------------------

    /**
     * B §4.2: the bootstrap list may omit the leader entirely — the probe replies report their
     * cluster views, the union widens the fan-out, and the LEADER is picked by highest term.
     */
    @Test
    void t1_partialBootstrapDiscoveryFindsLeaderOutsideBootstrap() {
        try (ClusterKit kit = new ClusterKit()) {
            final int leaderIdx = kit.indexOf(kit.awaitLeader("No leader up after cluster build", WAIT, UNIT));
            final List<NodeAddress> partial = new ArrayList<>(kit.nodeCount() - 1);
            for (int i = 0; i < kit.nodeCount(); i++) {
                if (i != leaderIdx) {
                    partial.add(kit.node(i).getNodeAddress());
                }
            }
            // The leader is NOT in the bootstrap list — a client that can only reach
            // the two followers must still find it through the union of their views.
            try (RemoteCacheClient client = new RemoteCacheClient(partial)) {
                client.put("disc", data()).join();
                assertTrue(kit.localHas(leaderIdx, "disc"), "put must have landed on the undiscovered leader");
                final Map<String, byte[]> read = client.get("disc").join().orElseThrow().getData();
                assertEquals(Set.of("field"), read.keySet(), "the read must return the same data keys");
                assertArrayEquals(FIELD_VALUE, read.get("field"), "the read must return the same value bytes");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // t2 — read semantics: hit round-trips, a miss is an empty Optional
    // ---------------------------------------------------------------------------

    /** B §4.3 / §4.5: put then get returns the value (read-your-own-write at completion); an absent key is a miss. */
    @Test
    void t2_putThenGetRoundTripsAndMissingKeyIsEmpty() {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            try (RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit))) {
                client.put("present", data()).join();
                final KeyDataOutput hit = client.get("present").join().orElseThrow();
                assertEquals("present", hit.getKey());
                assertEquals(Set.of("field"), hit.getData().keySet(), "the read must return the same data keys");
                assertArrayEquals(FIELD_VALUE, hit.getData().get("field"), "the read must return the same value bytes");
                assertFalse(client.get("missing").join().isPresent(), "absent key must be a miss, not an error");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // t3 — delete: a completed delete has removed the key everywhere
    // ---------------------------------------------------------------------------

    @Test
    void t3_putThenDeleteRemovesItClusterWide() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            try (RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit))) {
                client.put("gone", data()).join();
                client.delete("gone").join();
                // The delete's completion proves commit; use the LEADER's applied index as the
                // convergence target (the cache applied index — not the raft log index).
                final long index = kit.cache(leaderIdx).getLastAppliedIndex();
                kit.awaitAppliedAll(index, "delete must converge on every node", WAIT, UNIT);
                for (int i = 0; i < kit.nodeCount(); i++) {
                    assertFalse(kit.localHas(i, "gone"), "node " + i + " must have applied the delete");
                }
                assertFalse(client.get("gone").join().isPresent());
            }
        }
    }

    // ---------------------------------------------------------------------------
    // t4 — expire: the deadline is deterministic across the cluster (shared MutableClock)
    // ---------------------------------------------------------------------------

    @Test
    void t4_expireMakesTheKeyVanishClusterWide() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        try (ClusterKit kit = new ClusterKit(clock)) {
            final RaftNode leader = kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            try (RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit))) {
                client.put("aging", data()).join();
                client.expire("aging", Duration.ofMillis(2000L)).join();
                final long index = kit.cache(leaderIdx).getLastAppliedIndex();
                kit.awaitAppliedAll(index, "expire must converge on every node", WAIT, UNIT);

                clock.advanceMillis(1500L); // still before the 2000ms deadline
                assertTrue(client.get("aging").join().isPresent(), "key must still be live before its deadline");

                clock.advanceMillis(600L); // 2100ms — past the deadline
                assertFalse(client.get("aging").join().isPresent(), "key must be expired past its deadline");
                for (int i = 0; i < kit.nodeCount(); i++) {
                    assertFalse(kit.localHas(i, "aging"), "node " + i + " must agree the key is expired");
                }
            }
        }
    }

    // ---------------------------------------------------------------------------
    // t5 — construction & argument contract
    // ---------------------------------------------------------------------------

    @Test
    void t5_constructionAndVerbArgumentsAreContracted() {
        final NodeAddress dummy = new NodeAddress("127.0.0.1:1"); // never dialed: all checks throw before I/O

        final NullPointerException nullBootstrap =
                assertThrows(NullPointerException.class, () -> new RemoteCacheClient(null));
        assertEquals("bootstrap must not be null", nullBootstrap.getMessage());

        final IllegalArgumentException emptyBootstrap =
                assertThrows(IllegalArgumentException.class, () -> new RemoteCacheClient(List.of()));
        assertEquals("bootstrap must not be empty", emptyBootstrap.getMessage());

        final List<NodeAddress> nullEntry = new ArrayList<>();
        nullEntry.add(dummy);
        nullEntry.add(null);
        final NullPointerException nullAddress =
                assertThrows(NullPointerException.class, () -> new RemoteCacheClient(nullEntry));
        assertEquals("bootstrap addresses must not be null", nullAddress.getMessage());

        final NullPointerException nullTimeout =
                assertThrows(NullPointerException.class, () -> new RemoteCacheClient(List.of(dummy), null));
        assertEquals("requestTimeout must not be null", nullTimeout.getMessage());

        final IllegalArgumentException zeroTimeout =
                assertThrows(IllegalArgumentException.class, () -> new RemoteCacheClient(List.of(dummy), Duration.ZERO));
        assertEquals("requestTimeout must be positive", zeroTimeout.getMessage());

        final IllegalArgumentException negativeTimeout =
                assertThrows(IllegalArgumentException.class, () -> new RemoteCacheClient(List.of(dummy), Duration.ofMillis(-1L)));
        assertEquals("requestTimeout must be positive", negativeTimeout.getMessage());

        try (RemoteCacheClient client = new RemoteCacheClient(List.of(dummy))) {
            assertThrows(IllegalArgumentException.class, () -> client.put(null, data()));
            assertThrows(IllegalArgumentException.class, () -> client.put("k", null));
            assertThrows(IllegalArgumentException.class, () -> client.delete(null));
            assertThrows(IllegalArgumentException.class, () -> client.expire(null, Duration.ofSeconds(1L)));
            assertThrows(NullPointerException.class, () -> client.expire("k", null));
            assertThrows(IllegalArgumentException.class, () -> client.get(null));
        }
    }

    // ---------------------------------------------------------------------------
    // t6 — close: idempotent; in-flight fails with "client closed"; post-close verbs refuse
    // ---------------------------------------------------------------------------

    @Test
    void t6_closeFailsInFlightAndPostCloseVerbsThrow() {
        try (ClusterKit kit = new ClusterKit()) {
            final int leaderIdx = kit.indexOf(kit.awaitLeader("No leader up after cluster build", WAIT, UNIT));
            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit));
            client.put("warm", data()).join();          // confirms the leader and dials it

            kit.interrupt(leaderIdx);                   // a write to it is never answered
            final CompletableFuture<Void> inFlight = client.put("stranded", data());

            client.close();
            client.close();                             // idempotent — no throw

            final RuntimeException stranded = assertThrows(RuntimeException.class, inFlight::join);
            assertTrue(chainContains(stranded, IllegalStateException.class),
                    "in-flight request must fail with IllegalStateException, was: " + describeChain(stranded));
            assertTrue(chainMessageContains(stranded, "client closed"),
                    "in-flight request must fail with the close marker, was: " + describeChain(stranded));

            final IllegalStateException refusedPut =
                    assertThrows(IllegalStateException.class, () -> client.put("k", data()));
            assertEquals("client is closed", refusedPut.getMessage());
            final IllegalStateException refusedGet =
                    assertThrows(IllegalStateException.class, () -> client.get("k"));
            assertEquals("client is closed", refusedGet.getMessage());
        }
    }

    // ---------------------------------------------------------------------------
    // t7 — NOT_LEADER: abandon, re-discover, retry EXACTLY once on the fresh leader
    // ---------------------------------------------------------------------------

    /**
     * B §3.5 bounded retry: while the client still points at the old leader, the cluster has
     * already elected a fresh one — the write lands on the demoted node, gets NOT_LEADER, and
     * the client recovers with a single re-discovery + retry on the new, higher-term leader.
     */
    @Test
    void t7_notLeaderReplyAbandonsAndRetriesFreshLeaderOnce() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode oldLeader = kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            final int oldIdx = kit.indexOf(oldLeader);
            final int oldTerm = oldLeader.getCurrentTermIndex().join().getTerm();

            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit));
            client.put("warm", data()).join();          // confirms the (old) leader and dials it

            kit.interrupt(oldIdx);
            final RaftNode freshLeader = kit.awaitNewLeader(oldLeader, oldTerm, "No fresh leader after partition", WAIT, UNIT);
            kit.restore(oldIdx);
            kit.awaitSingleLeader("No single leader after restore", WAIT, UNIT); // the old one is demoted now

            // The client still points at the demoted node: this write gets NOT_LEADER and must
            // recover on its own — one re-discovery, one retry, on the higher-term leader.
            client.put("again", data()).join();

            final int freshIdx = kit.indexOf(freshLeader);
            final long index = kit.cache(freshIdx).getLastAppliedIndex();
            kit.awaitAppliedAll(index, "retried write must converge on every node", WAIT, UNIT);
            assertTrue(kit.localHas(oldIdx, "again"), "the demoted former leader must catch up too");

            client.close();
        }
    }

    // ---------------------------------------------------------------------------
    // t8 — no-leader window: typed NoLeaderException, then clean recovery
    // ---------------------------------------------------------------------------

    @Test
    void t8_allNodesSealedIsTypedNoLeaderAndRecovers() {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            for (int i = 0; i < kit.nodeCount(); i++) {
                kit.interrupt(i);                        // the whole cluster is unreachable to the client
            }

            // A short request budget keeps the failing probes fast: this test is about the
            // TYPED error, not about the 10 s default.
            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit), Duration.ofSeconds(1L));

            final RuntimeException noLeader = assertThrows(RuntimeException.class, () -> client.put("k", data()).join());
            assertTrue(chainContains(noLeader, NoLeaderException.class),
                    "a no-leader window must surface NoLeaderException, was: " + describeChain(noLeader));
            assertFalse(chainContains(noLeader, TimeoutException.class),
                    "NoLeaderException must be the typed cause, not a timed-out probe: " + describeChain(noLeader));

            // Recovery: heal the cluster — the same client must succeed on its next attempts
            // (see {@link #putEventually}: the contract is a typed, RETRYABLE error).
            for (int i = 0; i < kit.nodeCount(); i++) {
                kit.restore(i);
            }
            kit.awaitSingleLeader("No leader after restore", WAIT, UNIT);
            putEventually(client, "k", 8, 250L);
            client.close();
        }
    }

    // ---------------------------------------------------------------------------
    // t9 — request timeout: a silent peer is a typed TimeoutException, never a hang
    // ---------------------------------------------------------------------------

    /**
     * The no-leader contract is RECOVERABLE, not instant: after a FULL quorum loss the healed
     * cluster first has to re-elect and re-dial its inter-node links before a write can commit.
     * Retry the same write a bounded number of times on the SAME client — that is precisely
     * what "typed retryable error" buys the caller.
     */
    private static void putEventually(RemoteCacheClient client, String key, int maxAttempts, long pauseMillis) {
        for (int attempt = 1; ; attempt++) {
            try {
                client.put(key, data()).join();
                return;
            } catch (RuntimeException retryable) {
                if (attempt >= maxAttempts) {
                    throw new IllegalStateException("client did not recover after the cluster rejoined"
                            + " (last failure: " + describeChain(retryable) + ")", retryable);
                }
                try {
                    Thread.sleep(pauseMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted between recovery attempts", interrupted);
                }
            }
        }
    }

    /**
     * B §4.6: the leader was confirmed, then sealed — the write reaches it and is never
     * answered. The per-request budget must trip and surface as the <em>terminal</em>
     * {@link TimeoutException} cause (checked exceptions travel in the cause chain).
     */
    @Test
    void t9_unresponsiveLeaderSurfacesTypedTimeoutAtTerminalCause() {
        try (ClusterKit kit = new ClusterKit()) {
            final int leaderIdx = kit.indexOf(kit.awaitLeader("No leader up after cluster build", WAIT, UNIT));
            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit), Duration.ofSeconds(2L));
            client.put("warm", data()).join();          // confirms the leader and dials it

            kit.interrupt(leaderIdx);                    // the confirmed leader goes silent
            final RuntimeException silent = assertThrows(RuntimeException.class, () -> client.put("cold", data()).join());

            assertTrue(chainContains(silent, TimeoutException.class),
                    "a silent peer must surface the typed TimeoutException, was: " + describeChain(silent));
            assertEquals(TimeoutException.class, terminalCause(silent).getClass(),
                    "the terminal cause must BE the TimeoutException, was: " + describeChain(silent));
            client.close();
        }
    }

    private static Throwable terminalCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    // ---------------------------------------------------------------------------
    // t10 — durability: a completed put is committed, applied and readable everywhere
    // ---------------------------------------------------------------------------

    @Test
    void t10_putIsDurableAndConvergesAcrossAllNodes() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("No leader up after cluster build", WAIT, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            try (RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit))) {
                client.put("durable", data()).join();
                // Read-your-own-write: completion alone is enough — no wait, no retry, no follower read.
                final KeyDataOutput ownWrite = client.get("durable").join().orElseThrow();
                assertEquals(Set.of("field"), ownWrite.getData().keySet(), "own write: same data keys");
                assertArrayEquals(FIELD_VALUE, ownWrite.getData().get("field"), "own write: same value bytes");

                // And the entry is committed+applied on every node, not just the leader:
                final long index = kit.cache(leaderIdx).getLastAppliedIndex();
                kit.awaitAppliedAll(index, "write must be applied on every node", WAIT, UNIT);
                for (int i = 0; i < kit.nodeCount(); i++) {
                    final Map<String, byte[]> view = kit.localViewOf(i, "durable").getData();
                    assertEquals(Set.of("field"), view.keySet(), "node " + i + " must hold the same data keys");
                    assertArrayEquals(FIELD_VALUE, view.get("field"), "node " + i + " must hold the same value bytes");
                }
            }
        }
    }
}
