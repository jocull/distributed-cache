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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
