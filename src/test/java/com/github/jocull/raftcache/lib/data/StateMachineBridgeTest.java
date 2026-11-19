package com.github.jocull.raftcache.lib.data;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
