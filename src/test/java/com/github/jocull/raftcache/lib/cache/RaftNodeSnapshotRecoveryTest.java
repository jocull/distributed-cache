package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 5 of {@code SNAPSHOTS.md}, end to end over real sockets: a follower that falls
 * more than {@code SNAPSHOT_TRIGGER_ENTRIES} behind the committed point is recovered with
 * an {@code InstallSnapshot} instead of entry-by-entry replay, and once every follower is
 * back at the committed point the leader's log compacts to the snapshot boundary (S1).
 *
 * <p>Recipe:
 * <ol>
 *   <li>elect a leader, seal one follower — the live leader+follower pair is the 2/3
 *       majority, so writes keep committing (N4) while the sealed node's last acknowledged
 *       index stays frozen behind the commit point, exactly the laggard state the snapshot
 *       exists for;</li>
 *   <li>write eight entries — more than the 5-entry laggard trigger, so the leader's send
 *       path to the sealed follower is the {@code InstallSnapshot} branch, not appends;</li>
 *   <li>restore — the sealed follower must converge entirely through
 *       install(snapshot) + the appends behind its boundary, then all three local maps
 *       agree, versions included;</li>
 *   <li>write two more and let the cluster converge — now every live follower is at or
 *       past the committed point, so the leader's compact gate must pass and its log must
 *       carry a start-point snapshot.</li>
 * </ol>
 *
 * <p>Step 4 observes the leader's log state (compaction is internal housekeeping with no
 * client-visible effect yet), which is package-private — see
 * {@link #awaitLeaderCompacted(RaftNode)}.
 */
class RaftNodeSnapshotRecoveryTest {
    private static final int NODES = 3;
    private static final int WAIT_SECONDS = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    @Test
    void laggardRecoversThroughSnapshotAndLeaderCompactsAfterwards() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("no leader before the partition", WAIT_SECONDS, UNIT);
            final int leaderIndex = kit.indexOf(leader);

            // Seal one follower: the leader and the other follower form the live majority,
            // so writes commit (N4) while the sealed node's view stays frozen.
            final int partitioned = kit.indexOf(kit.followers().get(0));
            kit.interrupt(partitioned);

            // Eight writes (indices 1..8) commit on the live pair. The sealed node never
            // hears them; its 0-vs-8 gap exceeds the 5-entry laggard trigger, so any send to
            // it from now on is the InstallSnapshot branch.
            final long lastIndex = writeRange(kit, leaderIndex, 1, 8);
            for (int k = 1; k <= 8; k++) {
                assertFalse(kit.localHas(partitioned, "snap-" + k),
                        "the sealed node must not hold key snap-" + k + " (its view is frozen)");
            }
            for (int i = 0; i < NODES; i++) {
                if (i != partitioned) {
                    kit.awaitApplied(i, lastIndex, "live node " + i + " did not apply the committed writes", WAIT_SECONDS, UNIT);
                    for (int k = 1; k <= 8; k++) {
                        assertValue(kit, i, "snap-" + k, "value-" + k);
                    }
                }
            }

            // Restore: the sealed follower catches up through InstallSnapshot (the eight
            // entries' state) and any appends behind its boundary, then applies — all three
            // local maps hold every key with the same version (the snapshot carries the
            // version counters, so convergence is value- AND version-consistent).
            kit.restore(partitioned);
            kit.awaitSingleLeader("cluster did not return to exactly one leader after restore", WAIT_SECONDS, UNIT);
            kit.awaitApplied(partitioned, lastIndex,
                    "the sealed follower did not converge through the installed snapshot", WAIT_SECONDS, UNIT);
            for (int k = 1; k <= 8; k++) {
                for (int i = 0; i < NODES; i++) {
                    assertValue(kit, i, "snap-" + k, "value-" + k);
                }
            }

            // Two more writes: with every follower back at the committed point, the leader's
            // compact gate (SNAPSHOTS.md stage 5: S1 + all-followers-at-or-past-committed)
            // must pass and the leader's log must compact to the snapshot boundary.
            final long postIndex = writeRange(kit, leaderIndex, 9, 10);
            kit.awaitAppliedAll(postIndex, "the post-recovery writes did not apply on every node", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                assertEquals(10L, kit.localViewOf(i, "snap-10").getVersion(),
                        "node " + i + " must see ten applied entries as version 10");
            }

            assertTrue(awaitLeaderCompacted(kit.node(leaderIndex)),
                    "the leader's log still has no start-point snapshot even though every follower is at the committed point");
        }
    }

    private static long writeRange(ClusterKit kit, int leaderIndex, int firstKey, int lastKey) throws Exception {
        long lastIndex = 0;
        for (int k = firstKey; k <= lastKey; k++) {
            // apply-then-acknowledge: the put's future completes only after the leader has
            // applied the entry (and it committed on the live majority), so the captured
            // index is quorum-committed.
            lastIndex = kit.cache(leaderIndex)
                    .put("snap-" + k, Map.of("value", ("value-" + k).getBytes(StandardCharsets.UTF_8)))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
        }
        return lastIndex;
    }

    private static void assertValue(ClusterKit kit, int i, String key, String expectedValue) {
        assertEquals(expectedValue, new String(kit.localViewOf(i, key).getData().get("value"), StandardCharsets.UTF_8),
                "node " + i + " value for " + key);
    }

    /**
     * Polls the leader's log for a compacted start point. The compact gate is internal
     * state (its one client-visible effect — installing a snapshot on the laggard —
     * already happened in step 3), and the log type is package-private in
     * {@code lib.raft}, so the poll crosses the package boundary by reflection: read the
     * node's {@code logs} field, then call its public {@code hasStartPointSnapshot()}
     * without naming the class.
     */
    private static boolean awaitLeaderCompacted(RaftNode leader) {
        final Field logsField;
        try {
            logsField = leader.getClass().getDeclaredField("logs");
        } catch (NoSuchFieldException ex) {
            throw new AssertionError("No logs field on " + leader.getClass().getName(), ex);
        }
        logsField.setAccessible(true);

        final long deadline = System.currentTimeMillis() + WAIT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                final Object logs = logsField.get(leader);
                final Method hasSnapshot = logs.getClass().getMethod("hasStartPointSnapshot");
                hasSnapshot.setAccessible(true); // RaftLogs is package-private in lib.raft
                if (Boolean.TRUE.equals(hasSnapshot.invoke(logs))) {
                    return true;
                }
            } catch (ReflectiveOperationException ex) {
                throw new AssertionError("failed to inspect the leader's log state", ex);
            }
            sleepQuietly(50);
        }
        return false;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while polling the leader's log", ex);
        }
    }
}
