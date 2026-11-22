package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.MutableClock;
import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.events.SetDataEvent;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.events.SnapshotInstalled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StateMachineBridgeTest {
    private EventBus nodeEventBus;
    private PartitionMap partitionMap;
    private StateMachineBridge bridge;

    @BeforeEach
    void setUp() {
        // The PartitionMap's own event bus is unrelated to the node bus the bridge watches
        partitionMap = new PartitionMap(new EventBus(), 4);
        nodeEventBus = new EventBus();
        bridge = new StateMachineBridge(nodeEventBus, partitionMap);
    }

    private KeyDataInput input(String key, String value) {
        return new KeyDataInput(key, Map.of("value", value.getBytes(StandardCharsets.UTF_8)));
    }

    private void commit(RaftLog<?>... logs) {
        nodeEventBus.publish(new LogsCommitted(List.of(logs)));
    }

    @Test
    void testKeyDataInputIsAppliedToThePartitionMap() {
        final KeyDataInput data = input("key1", "value1");
        commit(new RaftLog<>(new TermIndex(1, 1), data));

        final Optional<KeyDataOutput> output = partitionMap.getData("key1");
        assertTrue(output.isPresent(), "Expected key1 to be present after its log was committed");
        assertEquals("key1", output.get().getKey());
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), output.get().getData().get("value"));
        assertEquals(1L, bridge.getLastAppliedIndex());
    }

    @Test
    void testKeyDataDeleteInputRemovesTheEntry() {
        commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        assertTrue(partitionMap.getData("key1").isPresent(), "Setup: key1 should be present");

        commit(new RaftLog<>(new TermIndex(1, 2), new KeyDataDeleteInput("key1")));

        assertTrue(partitionMap.getData("key1").isEmpty(), "key1 should be gone after the delete entry was committed");
        assertEquals(2L, bridge.getLastAppliedIndex());
    }

    @Test
    void testDeleteOfAbsentKeyIsANoOp() {
        // Deleting a key that was never written must not fail and must not block later entries
        commit(new RaftLog<>(new TermIndex(1, 1), new KeyDataDeleteInput("never-written")));
        commit(new RaftLog<>(new TermIndex(1, 2), input("key1", "value1")));

        assertTrue(partitionMap.getData("never-written").isEmpty());
        assertTrue(partitionMap.getData("key1").isPresent(), "Data entries after an absent-key delete should still be applied");
        assertEquals(2L, bridge.getLastAppliedIndex());
    }

    @Test
    void testDeleteThenReputAppliesInOrder() {
        // put -> delete -> put must replay to "present, latest value" (idempotent under re-delivery)
        final List<RaftLog<?>> batch = List.of(
                new RaftLog<>(new TermIndex(1, 1), input("key1", "v1")),
                new RaftLog<>(new TermIndex(1, 2), new KeyDataDeleteInput("key1")),
                new RaftLog<>(new TermIndex(1, 3), input("key1", "v2")));
        commit(batch.toArray(new RaftLog[0]));
        final long version = partitionMap.getData("key1").get().getVersion();

        // Re-delivering the whole batch (shared in-process bus) must not re-apply any of it
        commit(batch.toArray(new RaftLog[0]));

        assertTrue(partitionMap.getData("key1").isPresent(), "Key should be present again after the re-put");
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), partitionMap.getData("key1").get().getData().get("value"));
        assertEquals(version, partitionMap.getData("key1").get().getVersion(), "Duplicate delivery should not re-apply the put/delete/put");
        assertEquals(3L, bridge.getLastAppliedIndex());
    }

    @Test
    void testNonDataEntriesAreAcknowledgedButIgnored() {
        final KeyDataInput data = input("key1", "value1");
        commit(new RaftLog<>(new TermIndex(1, 1), "not a state machine command"));
        commit(new RaftLog<>(new TermIndex(1, 2), data));

        assertEquals(2L, bridge.getLastAppliedIndex());
        assertTrue(partitionMap.getData("key1").isPresent(), "Data entries after non-data entries should still be applied");
    }

    @Test
    void testDuplicateCommitDeliveryIsAppliedOnlyOnce() {
        final KeyDataInput data = input("key1", "value1");
        final List<RaftLog<?>> batch = List.of(new RaftLog<>(new TermIndex(1, 1), data));

        nodeEventBus.publish(new LogsCommitted(batch));
        final long version = partitionMap.getData("key1").get().getVersion();

        // The same entry being committed again (as would happen on a shared in-process bus
        // where every node emits its own commit) must not re-apply it
        nodeEventBus.publish(new LogsCommitted(batch));
        nodeEventBus.publish(new LogsCommitted(List.of(new RaftLog<>(new TermIndex(1, 1), data))));

        assertEquals(version, partitionMap.getData("key1").get().getVersion(), "Duplicate delivery should not re-apply the entry");
        assertEquals(1L, bridge.getLastAppliedIndex());
    }

    @Test
    void testOutOfOrderOlderIndexIsSkipped() {
        final KeyDataInput data1 = input("key1", "value1");
        final KeyDataInput data2 = input("key2", "value2");
        commit(new RaftLog<>(new TermIndex(1, 1), data1), new RaftLog<>(new TermIndex(1, 2), data2));

        final long version2 = partitionMap.getData("key2").get().getVersion();
        // A late/out-of-order event carrying an already-applied index must not re-apply it
        nodeEventBus.publish(new LogsCommitted(List.of(new RaftLog<>(new TermIndex(2, 1), input("key2", "stale")))));

        assertEquals(version2, partitionMap.getData("key2").get().getVersion(), "Older index should be skipped");
        assertArrayEquals("value2".getBytes(StandardCharsets.UTF_8), partitionMap.getData("key2").get().getData().get("value"));
        assertEquals(2L, bridge.getLastAppliedIndex());
    }

    @Test
    void testUnrelatedEventsAreIgnored() {
        final SetDataEvent unrelated = new SetDataEvent("key1", Map.of("value", "value1".getBytes(StandardCharsets.UTF_8)), 1L);
        nodeEventBus.publish(unrelated);

        assertTrue(partitionMap.getData("key1").isEmpty(), "Unrelated events must not touch the state machine");
        assertEquals(0L, bridge.getLastAppliedIndex());
    }

    @Test
    void testCloseUnsubscribesFromTheEventBus() {
        bridge.close();

        commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));

        assertTrue(partitionMap.getData("key1").isEmpty(), "After close, committed logs should no longer be applied");
        assertEquals(0L, bridge.getLastAppliedIndex());
    }

    // ---- EXPIRE over Raft (Signed-off work plan, item 2/TTL) ----
    //
    // These tests use their own map+bridge pair built on a deterministic MutableClock: the
    // bridge test above uses a real-clock map, which is fine for SET/DELETE but makes TTL
    // timing non-deterministic. Here the clock starts at Instant.EPOCH and the test advances
    // it explicitly, so expiration is pinned with no wall-clock sleeping.

    private static KeyDataExpireInput expire(String key, long ttlMillis) {
        return new KeyDataExpireInput(key, Duration.ofMillis(ttlMillis));
    }

    /** A self-contained state-machine pair wired to a deterministic clock. */
    private static final class TimedStateMachine {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        final EventBus mapEventBus = new EventBus();
        final PartitionMap map = new PartitionMap(mapEventBus, 4, clock);
        final EventBus nodeEventBus = new EventBus();
        final StateMachineBridge bridge = new StateMachineBridge(nodeEventBus, map);

        void commit(RaftLog<?>... logs) {
            nodeEventBus.publish(new LogsCommitted(List.of(logs)));
        }

        void advanceMillis(long millis) {
            clock.advanceMillis(millis);
        }
    }

    @Test
    void testExpireMakesTheKeyAbsentOnceItsTtlHasPassed() {
        final TimedStateMachine s = new TimedStateMachine();
        s.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        assertTrue(s.map.getData("key1").isPresent(), "Setup: key1 should be present after the put");

        // EXPIRE with a 5000ms TTL applied at clock 0 -> expires at epoch-millis 5000
        s.commit(new RaftLog<>(new TermIndex(1, 2), expire("key1", 5000L)));

        // The value is unchanged by EXPIRE -- it still reads the original data before the deadline
        final KeyDataOutput before = s.map.getData("key1")
                .orElseThrow(() -> new AssertionError("key1 should still be present before its deadline"));
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), before.getData().get("value"),
                "EXPIRE must not alter the stored data, only its expiry");

        // At the boundary the entry is already considered expired (expiresAt <= now)
        s.advanceMillis(4999L);
        assertTrue(s.map.getData("key1").isPresent(), "key1 must still be present just before its deadline");
        s.advanceMillis(1L); // now == 5000
        assertTrue(s.map.getData("key1").isEmpty(), "key1 must be absent once its TTL has elapsed");
        assertEquals(2L, s.bridge.getLastAppliedIndex());
    }

    @Test
    void testExpireOfAbsentKeyIsANoOp() {
        final TimedStateMachine s = new TimedStateMachine();

        // An EXPIRE on a key that was never written must not fail, not create an entry,
        // and must still advance the applied index so later entries are not blocked
        s.commit(new RaftLog<>(new TermIndex(1, 1), expire("never-written", 5000L)));
        assertTrue(s.map.getData("never-written").isEmpty(), "EXPIRE of an absent key must not create an entry");
        assertEquals(1L, s.bridge.getLastAppliedIndex());
        s.advanceMillis(6000L); // far past any TTL
        assertTrue(s.map.getData("never-written").isEmpty(), "A late clock must not resurrect an absent key");

        // A genuine put afterwards must still work and be visible
        s.commit(new RaftLog<>(new TermIndex(1, 2), input("never-written", "value1")));
        assertTrue(s.map.getData("never-written").isPresent(), "A put after an absent-key EXPIRE should apply");
        assertEquals(2L, s.bridge.getLastAppliedIndex());
    }

    @Test
    void testLateExpireCannotResurrectAnAlreadyExpiredKey() {
        final TimedStateMachine s = new TimedStateMachine();
        s.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        s.commit(new RaftLog<>(new TermIndex(1, 2), expire("key1", 5000L)));

        // Let the first EXPIRE lapse: at the apply time of the late EXPIRE the entry is already expired
        s.advanceMillis(6000L);
        assertTrue(s.map.getData("key1").isEmpty(), "Setup: key1 should be expired before the late EXPIRE");

        s.commit(new RaftLog<>(new TermIndex(1, 3), expire("key1", 5000L)));
        assertTrue(s.map.getData("key1").isEmpty(),
                "A late EXPIRE on an already-expired key must not resurrect it");
        assertEquals(3L, s.bridge.getLastAppliedIndex());
    }

    @Test
    void testSecondExpireExtendsTheDeadline() {
        final TimedStateMachine s = new TimedStateMachine();
        s.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        s.commit(new RaftLog<>(new TermIndex(1, 2), expire("key1", 5000L))); // deadline -> 5000

        // Still live at 3000; a second EXPIRE of 10000ms applied there pushes the deadline to 13000
        s.advanceMillis(3000L);
        s.commit(new RaftLog<>(new TermIndex(1, 3), expire("key1", 10000L)));

        // Past the first deadline (5000) but within the extended one (13000) -> still present
        s.advanceMillis(3000L); // now == 6000
        assertTrue(s.map.getData("key1").isPresent(),
                "key1 must survive past the first deadline because the second EXPIRE extended it");
        // Past the extended deadline -> absent
        s.advanceMillis(7001L); // now == 13001
        assertTrue(s.map.getData("key1").isEmpty(), "key1 must be absent past the extended deadline");
    }

    // ---- SNAPSHOTS.md stage 1 (capture/replace) ----
    //
    // Bridge-level: the snapshot's boundary (term, index of the last applied entry), the
    // adopt/replace semantics, and the replay guard that makes re-delivery of the entries
    // behind the boundary idempotent. Capture/replace at the map level lives in PartitionMapTest.

    @Test
    void testSnapshotRecordGuards() {
        assertThrows(NullPointerException.class, () -> new Snapshot(1L, 1L, null));
        assertThrows(IllegalArgumentException.class, () -> new Snapshot(-1L, 1L, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new Snapshot(1L, -1L, Map.of()));

        // The entries map is a detached copy: mutating the caller's map does not disturb the snapshot
        final Map<String, DataVersion> live = new HashMap<>();
        live.put("key1", new DataVersion(Map.of("value", new byte[]{'v'}), 1L));
        final Snapshot snapshot = new Snapshot(1L, 1L, live);
        live.clear();
        assertEquals(1, snapshot.size(), "The snapshot's entries must be a detached copy");
        assertEquals(1L, snapshot.entry("key1").getVersion());
    }

    @Test
    void testGetSnapshotReportsStateAndLastAppliedBoundary() {
        commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        commit(new RaftLog<>(new TermIndex(2, 2), input("key2", "value2")));

        final Snapshot snapshot = bridge.getSnapshot();
        assertEquals(2L, snapshot.term(), "boundary term = the term of the last applied entry");
        assertEquals(2L, snapshot.index(), "boundary index = the last applied index");
        assertEquals(2, snapshot.size());
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), snapshot.entry("key1").getData().get("value"));
        assertEquals(bridge.getLastAppliedIndex(), snapshot.index());
        assertEquals(bridge.getLastAppliedTerm(), snapshot.term());
    }

    @Test
    void testAdoptReinstallsStateAndMakesOldEntriesIdempotent() {
        final TimedStateMachine origin = new TimedStateMachine();
        origin.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "v1")));
        origin.commit(new RaftLog<>(new TermIndex(1, 2), input("key2", "v2")));
        origin.commit(new RaftLog<>(new TermIndex(1, 3), input("key1", "v3"))); // re-put of key1

        final Snapshot snapshot = origin.bridge.getSnapshot();
        assertEquals(3L, snapshot.index());
        assertEquals(1L, snapshot.term());
        final long key1Version = snapshot.entry("key1").getVersion();
        final long key2Version = snapshot.entry("key2").getVersion();

        // Origin moves on -- the live state and the snapshot diverge
        origin.commit(new RaftLog<>(new TermIndex(1, 4), input("key2", "changed")));
        origin.commit(new RaftLog<>(new TermIndex(1, 5), new KeyDataDeleteInput("key1")));
        assertTrue(origin.map.getData("key1").isEmpty(), "Setup: origin deleted key1 after the snapshot");

        // A lagging node installing the snapshot gets exactly its state
        final TimedStateMachine laggard = new TimedStateMachine();
        laggard.bridge.adopt(snapshot);
        assertArrayEquals("v3".getBytes(StandardCharsets.UTF_8),
                laggard.map.getData("key1").orElseThrow().getData().get("value"));
        assertEquals(key1Version, laggard.map.getData("key1").orElseThrow().getVersion());
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8),
                laggard.map.getData("key2").orElseThrow().getData().get("value"));
        assertEquals(key2Version, laggard.map.getData("key2").orElseThrow().getVersion());
        assertEquals(3L, laggard.bridge.getLastAppliedIndex(), "adopt must re-anchor the applied position");
        assertEquals(1L, laggard.bridge.getLastAppliedTerm());

        // Re-installing a snapshot at the same boundary is an idempotent re-install
        laggard.bridge.adopt(snapshot);
        assertEquals(key1Version, laggard.map.getData("key1").orElseThrow().getVersion(),
                "Re-adopt at the same boundary must not re-stamp versions");

        // Delivering the entries behind the boundary again is a no-op (the replay guard)
        laggard.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "v1")),
                new RaftLog<>(new TermIndex(1, 2), input("key2", "v2")),
                new RaftLog<>(new TermIndex(1, 3), input("key1", "v3")));
        assertEquals(key1Version, laggard.map.getData("key1").orElseThrow().getVersion(),
                "Entries at or below the boundary must be skipped, not re-applied");
        assertEquals(key2Version, laggard.map.getData("key2").orElseThrow().getVersion());

        // New writes after the install continue past the fast-forwarded version counter
        laggard.commit(new RaftLog<>(new TermIndex(2, 4), input("key1", "v4")));
        assertEquals(key1Version + 1, laggard.map.getData("key1").orElseThrow().getVersion(),
                "A post-adopt write must get a version above the restored ones");
        assertEquals(key2Version, laggard.map.getData("key2").orElseThrow().getVersion(),
                "An unrelated restored entry must be untouched");
    }

    @Test
    void testAdoptRefusesAStaleSnapshot() {
        commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        final long originalVersion = partitionMap.getData("key1").orElseThrow().getVersion();

        // The snapshot claims an older boundary (index 0) than what this node already applied
        final Snapshot stale = new Snapshot(1L, 0L, Map.of("key1", new DataVersion(Map.of("value", new byte[]{'o'}), 1L)));
        bridge.adopt(stale);

        assertEquals(1L, bridge.getLastAppliedIndex(), "A refused adopt must not move the applied position");
        assertEquals(originalVersion, partitionMap.getData("key1").orElseThrow().getVersion(),
                "A refused adopt must not re-stamp the state");
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                partitionMap.getData("key1").orElseThrow().getData().get("value"));
    }

    @Test
    void testCaptureExcludesAlreadyExpiredEntries() {
        final TimedStateMachine origin = new TimedStateMachine();
        origin.commit(new RaftLog<>(new TermIndex(1, 1), input("stay", "v1")));
        origin.commit(new RaftLog<>(new TermIndex(1, 2), input("drop", "v1")));
        origin.commit(new RaftLog<>(new TermIndex(1, 3), expire("drop", 1000L))); // deadline at epoch-millis 1000

        origin.advanceMillis(2000L); // "drop" is past its deadline at capture time
        final Snapshot snapshot = origin.bridge.getSnapshot();

        assertEquals(1, snapshot.size(), "An entry expired at capture time is not part of the state");
        assertEquals("v1", new String(snapshot.entry("stay").getData().get("value"), StandardCharsets.UTF_8));
        assertNull(snapshot.entry("drop"));

        // And a node installing it must never see the expired key
        final TimedStateMachine laggard = new TimedStateMachine();
        laggard.bridge.adopt(snapshot);
        assertTrue(laggard.map.getData("stay").isPresent());
        assertTrue(laggard.map.getData("drop").isEmpty(),
                "An entry expired at capture time must not be resurrected by the install");
    }

    @Test
    void testTtlSurvivesTheSnapshotHopAsAnAbsoluteDeadline() {
        final TimedStateMachine origin = new TimedStateMachine();
        origin.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        origin.commit(new RaftLog<>(new TermIndex(1, 2), expire("key1", 10000L))); // applied at clock 0 -> deadline 10000

        final Snapshot snapshot = origin.bridge.getSnapshot();
        assertEquals(10000L, snapshot.entry("key1").getExpiresAtEpochMillis(),
                "The captured deadline must be an absolute epoch-millis value, not a TTL");

        // The receiving node's clock is elsewhere (already 5000 in): the deadline is not
        // recomputed from its clock, so the entry is live until the absolute instant 10000
        final TimedStateMachine laggard = new TimedStateMachine();
        laggard.advanceMillis(5000L);
        laggard.bridge.adopt(snapshot);

        assertTrue(laggard.map.getData("key1").isPresent(), "Before the absolute deadline the entry must be live");
        laggard.advanceMillis(5000L); // now == 10000 (boundary-inclusive expiry)
        assertTrue(laggard.map.getData("key1").isEmpty(),
                "Past the absolute deadline the entry must be gone, regardless of the local clock's origin");
    }

    // ---- SNAPSHOTS.md stage 4 (install-snapshot wiring into the bridge) ----
    //
    // The follower that accepted an InstallSnapshot publishes SnapshotInstalled on its own
    // node event bus (see RaftNodeBehaviorFollowerInstallSnapshotTest); a bridge
    // subscribed to that bus must then adopt the payload and re-anchor its applied
    // position. A foreign payload on a shared bus is ignored, never fatal.

    @Test
    void testSnapshotInstalledEventAdoptsThePayload() {
        final TimedStateMachine origin = new TimedStateMachine();
        origin.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "v1")));
        origin.commit(new RaftLog<>(new TermIndex(1, 2), input("key2", "v2")));
        final Snapshot snapshot = origin.bridge.getSnapshot();
        final long key1Version = snapshot.entry("key1").getVersion();
        final long key2Version = snapshot.entry("key2").getVersion();

        // The follower accepted the snapshot and published the event on its own bus;
        // the bridge subscribed to that bus must install the snapshot's state
        final TimedStateMachine laggard = new TimedStateMachine();
        laggard.nodeEventBus.publish(new SnapshotInstalled(1, 2, snapshot));

        assertTrue(laggard.map.getData("key1").isPresent(),
                "A SnapshotInstalled carrying a Snapshot must adopt the state");
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8),
                laggard.map.getData("key1").orElseThrow().getData().get("value"));
        assertEquals(key1Version, laggard.map.getData("key1").orElseThrow().getVersion(),
                "The restored entry must carry its restored version");
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8),
                laggard.map.getData("key2").orElseThrow().getData().get("value"));
        assertEquals(key2Version, laggard.map.getData("key2").orElseThrow().getVersion(),
                "An unrelated restored entry must carry its restored version");
        assertEquals(2L, laggard.bridge.getLastAppliedIndex(),
                "The SnapshotInstalled event must re-anchor the applied position to the snapshot boundary");
        assertEquals(1L, laggard.bridge.getLastAppliedTerm(),
                "The applied term must be the snapshot's boundary term");

        // Delivering the entries behind the boundary again is a no-op (the replay guard)
        laggard.commit(new RaftLog<>(new TermIndex(1, 2), input("key2", "replayed")));
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8),
                laggard.map.getData("key2").orElseThrow().getData().get("value"),
                "Entries at or below the boundary must be skipped, not re-applied");
        assertEquals(key2Version, laggard.map.getData("key2").orElseThrow().getVersion(),
                "Entries at or below the boundary must not re-stamp versions");
        assertEquals(2L, laggard.bridge.getLastAppliedIndex(),
                "Replaying behind the boundary must not move the applied position");
    }

    @Test
    void testSnapshotInstalledWithAForeignPayloadIsIgnored() {
        final TimedStateMachine s = new TimedStateMachine();
        s.commit(new RaftLog<>(new TermIndex(1, 1), input("key1", "value1")));
        final long originalVersion = s.map.getData("key1").orElseThrow().getVersion();

        // Another state machine on the shared bus publishes a snapshot of a type this
        // bridge does not recognise: it must be logged and ignored, never fatal
        assertDoesNotThrow(() -> s.nodeEventBus.publish(new SnapshotInstalled(9, 9, "foreign payload")));

        assertEquals(1L, s.bridge.getLastAppliedIndex(),
                "A foreign payload must not move the applied position");
        assertEquals(originalVersion, s.map.getData("key1").orElseThrow().getVersion(),
                "A foreign payload must not re-stamp the state");
        assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                s.map.getData("key1").orElseThrow().getData().get("value"),
                "A foreign payload must not alter the state");

        // Normal processing continues: a genuine commit after the foreign event applies
        s.commit(new RaftLog<>(new TermIndex(1, 2), input("key2", "value2")));
        assertTrue(s.map.getData("key2").isPresent(),
                "Data entries after a foreign payload should still be applied");
        assertEquals(2L, s.bridge.getLastAppliedIndex());
    }
}
