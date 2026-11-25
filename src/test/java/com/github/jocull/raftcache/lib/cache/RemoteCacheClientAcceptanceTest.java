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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client B, stage B4 — the §5 acceptance bar of {@code REMOTE-CLIENT-B-CLIENT-RPC.md},
 * driven end-to-end by the production {@link RemoteCacheClient} over a real 3-node socket
 * cluster ({@link ClusterKit}) with zero {@code RaftNode} references client-side — exactly
 * the way an external process would run it.
 *
 * <p>What this stage adds on top of B3's unit-level coverage (verbs, NOT_LEADER retry,
 * no-leader and typed timeout — all live in {@code RemoteCacheClientTest}):
 * <ul>
 *   <li>{@link #fullVerbJourneyOverRealSockets} — the §5 "put → get → expire → delete"
 *       journey of a single client, with the write-durability guarantee (committed by
 *       majority, applied before the future completes) on every node;</li>
 *   <li>{@link #killedLeaderTypedFailureThenCommitsUnderFreshLeader} — the §5 failover
 *       bullet's KILL branch (B3's t7 was the depromote/NOT_LEADER branch): the leader is
 *       sealed dead, an in-window client write fails with the typed, retryable
 *       {@link TimeoutException}, the survivors elect a fresh leader under the
 *       at-most-one-leader-per-term guard, the SAME client recovers and commits under the
 *       new leader, and the pre-failure write survives on the live pair byte-exact;</li>
 *   <li>{@link #killedLeaderRestoredConvergesAndClientContinues} — restore the killed node:
 *       it rejoins as a follower, converges to the pair's state, and the client keeps
 *       working end-to-end (read-your-own-write) against the healed cluster.</li>
 * </ul>
 */
class RemoteCacheClientAcceptanceTest {
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

    private static Throwable terminalCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
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

    /**
     * The no-leader / failover contract is retryable, not instant: the write may legitimately
     * fail a bounded number of times while the cluster re-elects and the client re-discovers.
     * Retry it a bounded number of times on the SAME client — that is what "typed, retryable
     * error" buys the caller ({@code RemoteCacheClientTest.t8} uses the same helper shape).
     */
    private static void putEventually(RemoteCacheClient client, String key, int maxAttempts, long pauseMillis) {
        for (int attempt = 1; ; attempt++) {
            try {
                client.put(key, data()).join();
                return;
            } catch (RuntimeException retryable) {
                if (attempt >= maxAttempts) {
                    throw new IllegalStateException("client did not recover after the cluster healed"
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

    // ---------------------------------------------------------------------------
    // §5 bullet 1 — the documented verb journey, in order, over real sockets
    // ---------------------------------------------------------------------------

    /**
     * B §5: an external client does put → get → expire → delete and observes the documented
     * semantics — a completed write is committed by the majority and applied before its
     * future completes (read-your-own-write at completion), and every node converges on it.
     */
    @Test
    void fullVerbJourneyOverRealSockets() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        try (ClusterKit kit = new ClusterKit(clock)) {
            final RaftNode leader = kit.awaitLeader("B4: no leader up after cluster build", WAIT, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            try (RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit))) {
                client.put("journey", data()).join();
                final KeyDataOutput got = client.get("journey").join().orElseThrow();
                assertEquals(Set.of("field"), got.getData().keySet(), "journey: same data keys");
                assertArrayEquals(FIELD_VALUE, got.getData().get("field"), "journey: same value bytes");

                client.expire("journey", Duration.ofMillis(2000L)).join();
                kit.awaitAppliedAll(kit.cache(leaderIdx).getLastAppliedIndex(),
                        "B4: the expire must converge on every node", WAIT, UNIT);
                clock.advanceMillis(2100L); // past the deadline
                assertFalse(client.get("journey").join().isPresent(), "the expired key must be a miss");
                for (int i = 0; i < kit.nodeCount(); i++) {
                    assertFalse(kit.localHas(i, "journey"), "node " + i + " must agree the key is expired");
                }

                client.put("gone", data()).join();
                client.delete("gone").join();
                kit.awaitAppliedAll(kit.cache(leaderIdx).getLastAppliedIndex(),
                        "B4: the delete must converge on every node", WAIT, UNIT);
                for (int i = 0; i < kit.nodeCount(); i++) {
                    assertFalse(kit.localHas(i, "gone"), "node " + i + " must have applied the delete");
                }
                assertFalse(client.get("gone").join().isPresent(), "the deleted key must be a miss");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // §5 bullet "Leader failover" — the KILL branch: leader unresponsive, not just demoted
    // ---------------------------------------------------------------------------

    /**
     * B §5: kill (seal) the leader; the client's in-window write fails with the TYPED,
     * retryable {@link TimeoutException} (never a crash); the survivors elect a fresh leader
     * (the at-most-one-leader-per-term guard is asserted on every poll sample); the SAME
     * client re-discovers and commits under the new leader — and the write committed before
     * the kill survives byte-exact on the live pair.
     */
    @Test
    void killedLeaderTypedFailureThenCommitsUnderFreshLeader() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode oldLeader = kit.awaitLeader("B4: no leader before the kill", WAIT, UNIT);
            final int oldIdx = kit.indexOf(oldLeader);
            final int oldTerm = oldLeader.getCurrentTermIndex().join().getTerm();

            // A short request budget keeps the deterministic failure windows fast (this test
            // is about the typed recovery, not the 10 s default).
            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit), Duration.ofSeconds(2L));
            client.put("pre", data()).join();            // committed before the kill — must survive

            kit.interrupt(oldIdx);                        // the confirmed leader dies (two-way seal)

            final RuntimeException dead = assertThrows(RuntimeException.class,
                    () -> client.put("during", data()).join());
            assertEquals(TimeoutException.class, terminalCause(dead).getClass(),
                    "an in-window write against a dead leader must be the typed TimeoutException, was: "
                            + describeChain(dead));

            final RaftNode freshLeader = kit.awaitNewLeader(oldLeader, oldTerm,
                    "B4: the live pair did not elect after the leader was killed", WAIT, UNIT);
            final int freshIdx = kit.indexOf(freshLeader);

            putEventually(client, "during", 8, 250L);     // the same client recovers: re-discover + retry
            final long index = kit.cache(freshIdx).getLastAppliedIndex();
            // A sealed node cannot apply anything — wait only on the live pair (IT-6 shape).
            for (int i = 0; i < kit.nodeCount(); i++) {
                if (i != oldIdx) {
                    kit.awaitApplied(i, index, "B4: live node " + i + " did not apply the retried write", WAIT, UNIT);
                }
            }

            for (int i = 0; i < kit.nodeCount(); i++) {
                if (i == oldIdx) {
                    assertFalse(kit.localHas(oldIdx, "during"),
                            "the dead node's view must not hold the write committed without it");
                    final KeyDataOutput pre = kit.localViewOf(oldIdx, "pre");
                    assertArrayEquals(FIELD_VALUE, pre.getData().get("field"),
                            "the dead node must still hold the write it committed before dying, byte-exact");
                } else {
                    final KeyDataOutput during = kit.localViewOf(i, "during");
                    assertArrayEquals(FIELD_VALUE, during.getData().get("field"),
                            "node " + i + " must hold the retried write byte-exact");
                    final KeyDataOutput pre = kit.localViewOf(i, "pre");
                    assertArrayEquals(FIELD_VALUE, pre.getData().get("field"),
                            "node " + i + " must still hold the pre-failure write byte-exact");
                }
            }
            client.close();
        }
    }

    // ---------------------------------------------------------------------------
    // §5 bullet "Leader failover" + recovery — restore the killed node, converge, keep working
    // ---------------------------------------------------------------------------

    /**
     * B §5: the killed leader is restored — it rejoins as a follower (never a split-brain:
     * {@code awaitSingleLeader} re-asserts at-most-one-leader-per-term on every sample),
     * converges on the writes the live pair committed while it was dead, and the same client
     * keeps serving its journey (read-your-own-write) against the healed cluster.
     */
    @Test
    void killedLeaderRestoredConvergesAndClientContinues() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode oldLeader = kit.awaitLeader("B4: no leader before the kill", WAIT, UNIT);
            final int oldIdx = kit.indexOf(oldLeader);
            final int oldTerm = oldLeader.getCurrentTermIndex().join().getTerm();

            final RemoteCacheClient client = new RemoteCacheClient(allAddresses(kit), Duration.ofSeconds(2L));
            client.put("survivor", data()).join();

            kit.interrupt(oldIdx);
            final RaftNode freshLeader = kit.awaitNewLeader(oldLeader, oldTerm,
                    "B4: the live pair did not elect after the leader was killed", WAIT, UNIT);
            final int freshIdx = kit.indexOf(freshLeader);
            putEventually(client, "later", 8, 250L);       // committed under the fresh leader
            final long index = kit.cache(freshIdx).getLastAppliedIndex();
            for (int i = 0; i < kit.nodeCount(); i++) {    // the sealed leader cannot apply: live pair only
                if (i != oldIdx) {
                    kit.awaitApplied(i, index, "B4: live node " + i + " did not apply the new write", WAIT, UNIT);
                }
            }

            kit.restore(oldIdx);
            kit.awaitSingleLeader("B4: not exactly one leader after the killed leader rejoined", WAIT, UNIT);
            kit.awaitAppliedAll(index, "B4: the rejoined node must converge on both writes", WAIT, UNIT);
            for (int i = 0; i < kit.nodeCount(); i++) {
                for (String key : List.of("survivor", "later")) {
                    final KeyDataOutput view = kit.localViewOf(i, key);
                    assertArrayEquals(FIELD_VALUE, view.getData().get("field"),
                            "node " + i + " must hold " + key + " byte-exact after convergence");
                }
            }

            // The client's journey continues end-to-end against the healed cluster:
            client.put("post", data()).join();
            final KeyDataOutput post = client.get("post").join().orElseThrow();
            assertArrayEquals(FIELD_VALUE, post.getData().get("field"),
                    "client must keep its read-your-own-write after the kill/restore");
            client.close();
        }
    }
}
