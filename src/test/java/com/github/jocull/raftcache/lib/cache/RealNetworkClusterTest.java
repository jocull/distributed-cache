package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.MutableClock;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3, stage C — the integration tests of {@code NETWORKING.md} 4 (scenarios 1–6) re-proven
 * over a real wire: a production-shaped 3-node cluster where every Raft node sits behind its own
 * {@link com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware}
 * (ephemeral {@code 127.0.0.1} port), its own {@code RaftManager}/{@code EventBus}, its own
 * {@code PartitionMap} and its own {@code DistributedCache} — built and torn down by
 * {@link ClusterKit}.
 *
 * <p>The mocked-networking behaviors of items 1–3 are re-asserted here, one test per scenario:
 * <ol>
 *   <li>election over sockets — one leader, two followers, at-most-one-leader-per-term on
 *       <em>every</em> poll sample (the existing hardening rule);</li>
 *   <li>PUT replicates and applies on <em>all three</em> local state machines (distinct maps,
 *       same committed log), correct version;</li>
 *   <li>DELETE then re-PUT — delete visible on all three, version counter continues;</li>
 *   <li>EXPIRE — cluster-wide shared clock, present just short of the deadline, gone just past
 *       it, on every node's read path;</li>
 *   <li>partition a <em>follower</em> — writes keep committing on the live pair (N4), the
 *       partitioned view stays absent, restore → three-way convergence (N5);</li>
 *   <li>partition the <em>leader</em> — the live pair elects a new leader at a strictly higher
 *       term and keeps committing (N4); restore → the old leader is stepped down or re-elected
 *       at a strictly higher term (the {@code RaftNodeTest} rule), one leader overall,
 *       convergence (N5).</li>
 * </ol>
 *
 * <p>Value assertions compare the string content of the {@code value} field, never entry
 * equality ({@code KeyDataInput.equals} compares {@code byte[]} by reference identity —
 * {@code NETWORKING.md} follow-up #6); reads go through each node's own {@code DistributedCache}
 * (the local state machine's view).
 */
class RealNetworkClusterTest {
    private static final int NODES = 3;
    private static final int WAIT_SECONDS = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    @Test
    void it1ElectionOverSockets() {
        try (ClusterKit kit = new ClusterKit()) {
            // awaitLeader polls local getNodeState() and re-asserts at-most-one-leader-per-term
            // on every sample, so the split-brain safety of the election is covered in-line.
            final RaftNode leader = kit.awaitLeader("IT-1: no leader elected over real sockets", WAIT_SECONDS, UNIT);
            assertNotNull(leader, "IT-1: a leader must exist over sockets");
            assertEquals(1, kit.leaders().size(), "IT-1: exactly one leader");
            assertEquals(NODES - 1, kit.followers().size(), "IT-1: the other two nodes are followers");
        }
    }

    @Test
    void it2PutReplicatesAndAppliesOnAllNodes() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("IT-2: no leader before the write", WAIT_SECONDS, UNIT);

            // apply-then-acknowledge: the facade's future completes only after the leader has
            // applied the entry, so this index is the put itself on every local map.
            final long index = kit.cache(0).put("alice", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8)))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            kit.awaitAppliedAll(index, "IT-2: put not applied on every node", WAIT_SECONDS, UNIT);

            for (int i = 0; i < NODES; i++) {
                final KeyDataOutput view = kit.localViewOf(i, "alice");
                assertEquals("v1", new String(view.getData().get("value"), StandardCharsets.UTF_8),
                        "IT-2: node " + i + " value");
                assertEquals(1L, view.getVersion(),
                        "IT-2: node " + i + " must see the first applied entry as version 1");
            }
        }
    }

    @Test
    void it3DeleteThenRePut() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("IT-3: no leader before the writes", WAIT_SECONDS, UNIT);
            final DistributedCache cache = kit.cache(0);

            cache.put("bob", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8))).get(WAIT_SECONDS, UNIT);
            final long deleteIndex = cache.delete("bob").get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            kit.awaitAppliedAll(deleteIndex, "IT-3: delete not applied on every node", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "bob"), "IT-3: node " + i + " must see the delete");
            }

            // the version counter is per-map and global: every applied entry counts, so the
            // re-PUT lands on version 3 on every node (v1 -> delete -> v2).
            final long rePutIndex = cache.put("bob", Map.of("value", "v2".getBytes(StandardCharsets.UTF_8)))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            kit.awaitAppliedAll(rePutIndex, "IT-3: re-put not applied on every node", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                final KeyDataOutput view = kit.localViewOf(i, "bob");
                assertEquals("v2", new String(view.getData().get("value"), StandardCharsets.UTF_8),
                        "IT-3: node " + i + " value after re-put");
                assertEquals(3L, view.getVersion(),
                        "IT-3: node " + i + " version must continue past the delete");
            }
        }
    }

    @Test
    void it4ExpireAcrossCluster() throws Exception {
        // One clock for the whole cluster: expiration is resolved at read time against this
        // clock, so advancing it makes the expiry deterministic without sleeping.
        final MutableClock clusterClock = new MutableClock(Instant.EPOCH);
        try (ClusterKit kit = new ClusterKit(clusterClock)) {
            kit.awaitLeader("IT-4: no leader before the writes", WAIT_SECONDS, UNIT);
            final DistributedCache cache = kit.cache(0);

            cache.put("ttl-key", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8))).get(WAIT_SECONDS, UNIT);
            // EXPIRE is a replicated write; its future completes after the leader applied it,
            // pinning the deadline at clock 0 + 2s.
            final long expireIndex = cache.expire("ttl-key", Duration.ofSeconds(2L))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            kit.awaitAppliedAll(expireIndex, "IT-4: expire not applied on every node", WAIT_SECONDS, UNIT);

            for (int i = 0; i < NODES; i++) {
                assertTrue(kit.localHas(i, "ttl-key"), "IT-4: node " + i + " must still hold the key at its deadline");
            }

            clusterClock.advanceMillis(1500L); // still before the 2000ms deadline
            for (int i = 0; i < NODES; i++) {
                assertTrue(kit.localHas(i, "ttl-key"),
                        "IT-4: node " + i + " must still hold the key just before its deadline");
            }

            clusterClock.advanceMillis(600L); // 2100ms — past the deadline
            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "ttl-key"),
                        "IT-4: node " + i + " must read the key as expired on its checked-read path");
            }

            // EXPIRE of an absent key is a no-op through the facade: completes normally, no entry created.
            cache.expire("never-written", Duration.ofSeconds(2L)).get(WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "never-written"),
                        "IT-4: EXPIRE of an absent key must not create an entry on node " + i);
            }
        }
    }

    @Test
    void it5PartitionAFollower() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("IT-5: no leader before the partition", WAIT_SECONDS, UNIT);

            // Partition a follower: the leader and the other follower form the live majority,
            // so writes keep committing (N4) while the sealed node's view stays behind.
            final int partitioned = kit.indexOf(kit.followers().get(0));
            kit.interrupt(partitioned);

            final long index = kit.cache(0).put("part-key", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8)))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            for (int i = 0; i < NODES; i++) {
                if (i != partitioned) {
                    kit.awaitApplied(i, index, "IT-5: live node " + i + " did not apply the committed write", WAIT_SECONDS, UNIT);
                }
            }
            for (int i = 0; i < NODES; i++) {
                if (i == partitioned) {
                    assertFalse(kit.localHas(partitioned, "part-key"),
                            "IT-5: the partitioned node's view must still be absent of the committed write");
                } else {
                    assertTrue(kit.localHas(i, "part-key"), "IT-5: live node " + i + " must hold the committed write");
                }
            }

            // Restore the partition: the sealed follower catches up over the resumed channel (N5).
            kit.restore(partitioned);
            kit.awaitApplied(partitioned, index,
                    "IT-5: partitioned follower did not converge after restore", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                final KeyDataOutput view = kit.localViewOf(i, "part-key");
                assertEquals("v1", new String(view.getData().get("value"), StandardCharsets.UTF_8),
                        "IT-5: node " + i + " converged view");
            }
        }
    }

    @Test
    void it6PartitionTheLeader() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode oldLeader = kit.awaitLeader("IT-6: no leader before the partition", WAIT_SECONDS, UNIT);
            final int oldTerm = oldLeader.getCurrentTermIndex().join().getTerm();
            final int leaderIndex = kit.indexOf(oldLeader);

            kit.interrupt(leaderIndex);

            // The survivors' terms are <= oldTerm, so their fresh election is strictly higher —
            // the only way they can legally split the cluster and keep committing (N4).
            final RaftNode newLeader = kit.awaitNewLeader(oldLeader, oldTerm,
                    "IT-6: live pair did not elect a new leader after the leader was sealed", WAIT_SECONDS, UNIT);

            // The facade is refused here (two LEADERs across terms => "Multiple leaders
            // reported"), so the live pair commits through the new leader directly, exactly
            // like the mocked leader-partition scenario in RaftNodeTest.
            final long index = newLeader
                    .submit(new KeyDataInput("live-pair", Map.of("value", "v1".getBytes(StandardCharsets.UTF_8))))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            for (int i = 0; i < NODES; i++) {
                if (i == leaderIndex) {
                    assertFalse(kit.localHas(i, "live-pair"),
                            "IT-6: the sealed old leader's view must stay absent of the live pair's write");
                } else {
                    kit.awaitApplied(i, index, "IT-6: live node " + i + " did not apply the committed write", WAIT_SECONDS, UNIT);
                    assertTrue(kit.localHas(i, "live-pair"), "IT-6: live node " + i + " must hold the committed write");
                }
            }

            kit.restore(leaderIndex);
            kit.awaitSingleLeader("IT-6: cluster did not return to exactly one leader after restore", WAIT_SECONDS, UNIT);

            // The RaftNodeTest post-restore rule: the old leader has stepped down upon hearing
            // the higher term — or it won a fresh election, which can only be at a strictly
            // higher term (votes cannot be granted again at its old term).
            if (NodeStates.LEADER.equals(oldLeader.getNodeState().join())) {
                assertTrue(oldLeader.getCurrentTermIndex().join().getTerm() > oldTerm,
                        "IT-6: the old leader may only be leader again at a strictly higher term");
            }

            // N5 — three-way convergence, version-consistent across all three local maps.
            kit.awaitApplied(leaderIndex, index,
                    "IT-6: old leader did not converge after restore", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                final KeyDataOutput view = kit.localViewOf(i, "live-pair");
                assertEquals("v1", new String(view.getData().get("value"), StandardCharsets.UTF_8),
                        "IT-6: node " + i + " converged view");
            }
            final long version = kit.localViewOf(0, "live-pair").getVersion();
            for (int i = 1; i < NODES; i++) {
                assertEquals(version, kit.localViewOf(i, "live-pair").getVersion(),
                        "IT-6: version counters must agree across all three maps on node " + i);
            }
        }
    }
}
