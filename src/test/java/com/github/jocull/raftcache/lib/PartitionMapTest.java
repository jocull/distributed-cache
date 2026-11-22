package com.github.jocull.raftcache.lib;

import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.DataVersion;
import com.github.jocull.raftcache.lib.data.Partition;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.event.events.DeleteDataEvent;
import com.github.jocull.raftcache.lib.event.events.SetDataEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionMapTest {
    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
    }

    @Test
    void testPartitionSize() {
        final PartitionMap pm1 = new PartitionMap(eventBus, 16);
        final PartitionMap pm2 = new PartitionMap(eventBus, 256);
        final PartitionMap pm3 = new PartitionMap(eventBus, 1024);
        final PartitionMap pm4 = new PartitionMap(eventBus, 1);

        assertEquals(16, pm1.getPartitionCount());
        assertEquals(256, pm2.getPartitionCount());
        assertEquals(1024, pm3.getPartitionCount());
        assertEquals(1, pm4.getPartitionCount());

        assertThrows(IllegalArgumentException.class, () -> new PartitionMap(eventBus, 0));
        assertThrows(IllegalArgumentException.class, () -> new PartitionMap(eventBus, -10));
    }

    @Test
    void testPartitionForKey() {
        final PartitionMap pm = new PartitionMap(eventBus, 16);
        assertEquals(new Partition(2), pm.getPartitionForKey("hello"));
        assertEquals(new Partition(11), pm.getPartitionForKey("542342342342342342344"));
    }

    @Test
    void testPartitionDistribution() {
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final Map<Partition, List<Partition>> keys = IntStream.range(0, 10000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(pm::getPartitionForKey)
                .collect(Collectors.groupingBy(Function.identity()));

        keys.forEach((k, v) -> {
            assertTrue(v.size() > 10, "Key distribution @ " + k + " weak w/ size " + v.size());
        });
    }

    @Test
    void testDataGetSet() {
        final Random random = new Random();
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final List<KeyDataInput> inputs = IntStream.range(0, 1000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(key -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyDataInput(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        inputs.forEach(pm::setData);
        inputs.forEach(input -> {
            final String key = input.getKey();
            final KeyDataOutput output = pm.getData(key).orElseThrow();
            assertEquals(input.getKey(), output.getKey());
            assertEquals(input.getData(), output.getData());
        });
    }

    @Test
    void testReplaceData() {
        final Random random = new Random();
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final String key = UUID.randomUUID().toString();
        final List<KeyDataInput> inputs = IntStream.range(0, 1000)
                .mapToObj(i -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyDataInput(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        inputs.forEach(pm::setData);
        inputs.subList(0, inputs.size() - 1).forEach(input -> {
            final KeyDataOutput output = pm.getData(key).orElseThrow();
            assertEquals(input.getKey(), output.getKey());
            assertNotEquals(input.getData(), output.getData());
        });
        inputs.subList(inputs.size() - 1, inputs.size()).forEach(input -> {
            final KeyDataOutput output = pm.getData(key).orElseThrow();
            assertEquals(input.getKey(), output.getKey());
            assertEquals(input.getData(), output.getData());
        });
    }

    // ---- SNAPSHOTS.md stage 1 (capture/replace) ----
    //
    // Map-level (no Raft): the capture is a detached, full state (data + version, unlike the
    // read view) and the restore is a silent full replacement that fast-forwards the
    // version counter. The bridge-level adopt/replay-guard tests live in StateMachineBridgeTest.

    @Test
    void testCaptureIsADetachedCopyOfTheLiveState() {
        final Random random = new Random();
        final PartitionMap pm = new PartitionMap(eventBus, 256);
        final List<KeyDataInput> inputs = IntStream.range(0, 1000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(key -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyDataInput(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        inputs.forEach(pm::setData);

        // The capture is the full state (data + version) -- the same state the read view serves
        final Map<String, DataVersion> capture = pm.capture();
        assertEquals(1000, capture.size());
        inputs.forEach(input -> {
            final KeyDataOutput output = pm.getData(input.getKey()).orElseThrow();
            final DataVersion captured = capture.get(input.getKey());
            assertEquals(input.getData(), captured.getData());
            assertEquals(output.getVersion(), captured.getVersion());
        });

        // Mess with the PartitionMap entirely. The capture should still be valid.
        final int initialCaptureSize = capture.size();
        final List<KeyDataInput> inputs2 = IntStream.range(0, 1000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(key -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyDataInput(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        inputs2.forEach(pm::setData);

        // Nothing about the capture should have changed and no later key may have crept into it
        assertEquals(initialCaptureSize, capture.size());
        inputs.forEach(input -> assertEquals(input.getData(), capture.get(input.getKey()).getData()));
        inputs2.forEach(input -> assertNull(capture.get(input.getKey()),
                "A key written after the capture must not appear in it"));

        // Clear everything from the data set
        inputs.forEach(i -> pm.deleteData(i.getKey()));
        inputs2.forEach(i -> pm.deleteData(i.getKey()));

        inputs.forEach(i -> assertEquals(Optional.empty(), pm.getData(i.getKey())));
        inputs2.forEach(i -> assertEquals(Optional.empty(), pm.getData(i.getKey())));

        // Again, nothing about the capture should have changed
        assertEquals(initialCaptureSize, capture.size());
        inputs.forEach(input -> assertEquals(input.getData(), capture.get(input.getKey()).getData()));
    }

    @Test
    void testRestoreReplacesTheFullStateSilentlyAndFastForwardsTheCounter() {
        final EventBus sourceBus = new EventBus();
        final EventBus targetBus = new EventBus();
        final List<SetDataEvent> targetSetEvents = new ArrayList<>();
        final List<DeleteDataEvent> targetDeleteEvents = new ArrayList<>();
        targetBus.subscribe(new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof SetDataEvent) {
                    targetSetEvents.add((SetDataEvent) event);
                }
                if (event instanceof DeleteDataEvent) {
                    targetDeleteEvents.add((DeleteDataEvent) event);
                }
            }
        });

        // Source state: 500 keys, stamped with versions 1..500
        final PartitionMap source = new PartitionMap(sourceBus, 256);
        final List<KeyDataInput> inputs = IntStream.range(0, 500)
                .mapToObj(i -> new KeyDataInput(UUID.randomUUID().toString(), Map.of("data", new byte[]{'a'})))
                .collect(Collectors.toList());
        inputs.forEach(source::setData);
        final Map<String, DataVersion> capture = source.capture();
        assertEquals(500, capture.size());

        // Target with a stale key the snapshot's state does not contain
        final PartitionMap target = new PartitionMap(targetBus, 256);
        target.setData(new KeyDataInput("stale", Map.of("data", new byte[2])));
        assertEquals(1, targetSetEvents.size(), "Setup: the stale write should have been observed");

        target.restore(capture);

        // Restore is the silent path: reinstalls state, performs no new state-machine write
        assertEquals(1, targetSetEvents.size(), "restore must not publish a SetDataEvent");
        assertEquals(0, targetDeleteEvents.size(), "restore must not publish a DeleteDataEvent");

        // Full replacement: captured keys present with captured data + version, stale key dropped
        assertTrue(target.getData("stale").isEmpty(), "A key not in the snapshot's state must be dropped");
        inputs.forEach(input -> {
            final KeyDataOutput output = target.getData(input.getKey()).orElseThrow();
            assertEquals(capture.get(input.getKey()).getData(), output.getData());
            assertEquals(capture.get(input.getKey()).getVersion(), output.getVersion());
        });

        // Fast-forwarded counter: the next target write is strictly newer than every captured version
        final long restoredMax = capture.values().stream().mapToLong(DataVersion::getVersion).max().orElseThrow();
        target.setData(inputs.get(0));
        assertEquals(restoredMax + 1, target.getData(inputs.get(0).getKey()).orElseThrow().getVersion(),
                "A write after restore must get a version greater than every captured one");
    }

    @Test
    void testSubscribers() {
        final Random random = new Random();
        final PartitionMap pm = new PartitionMap(eventBus, 256);


        final List<SetDataEvent> setDataEvents = new ArrayList<>();
        final List<DeleteDataEvent> deleteDataEvents = new ArrayList<>();
        final EventSubscriber eventSubscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof SetDataEvent) {
                    setDataEvents.add((SetDataEvent) event);
                }
                if (event instanceof DeleteDataEvent) {
                    deleteDataEvents.add((DeleteDataEvent) event);
                }
            }
        };
        pm.getEventBus().subscribe(eventSubscriber);

        final List<KeyDataInput> inputs = IntStream.range(0, 1000)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(key -> {
                    final byte[] data = new byte[32];
                    random.nextBytes(data);
                    return new KeyDataInput(key, Map.of("data", data));
                })
                .collect(Collectors.toList());

        assertEquals(0, setDataEvents.size());
        assertEquals(0, deleteDataEvents.size());

        inputs.forEach(pm::setData);
        assertEquals(1000, setDataEvents.size());
        assertEquals(0, deleteDataEvents.size());
        for (int i = 0; i < inputs.size(); i++) {
            final KeyDataInput input = inputs.get(i);
            final SetDataEvent event = setDataEvents.get(i);
            assertEquals(input.getKey(), event.getKey());
            assertEquals(input.getData(), event.getData());
            assertEquals(i + 1, event.getVersion());
        }

        inputs.forEach(i -> pm.deleteData(i.getKey()));
        assertEquals(1000, setDataEvents.size());
        assertEquals(1000, deleteDataEvents.size());
        for (int i = 0; i < inputs.size(); i++) {
            final KeyDataInput input = inputs.get(i);
            final DeleteDataEvent event = deleteDataEvents.get(i);
            assertEquals(input.getKey(), event.getKey());
            assertEquals(i + 1001, event.getVersion());
        }

        // Deleting data that doesn't exist doesn't emit events
        inputs.forEach(i -> pm.deleteData(i.getKey()));
        assertEquals(1000, setDataEvents.size());
        assertEquals(1000, deleteDataEvents.size());

        // After unsubscribing no events should come through
        pm.getEventBus().unsubscribe(eventSubscriber);
        inputs.forEach(pm::setData);
        inputs.forEach(i -> pm.deleteData(i.getKey()));

        assertEquals(1000, setDataEvents.size());
        assertEquals(1000, deleteDataEvents.size());
    }

    // ---- TTL/EXPIRE (Signed-off work plan, item 2) ----
    //
    // Map-level (no Raft): the data-layer TTL semantics -- checked read, no-resurrection on a
    // late EXPIRE, and the sweep that actually frees memory and emits DeleteDataEvent.

    private KeyDataInput input(String key, String value) {
        return new KeyDataInput(key, Map.of("data", value.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testExpireMakesTheEntryServeAsAbsentAfterItsTtlAndSweepRemovesIt() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        final List<DeleteDataEvent> deleteEvents = new ArrayList<>();
        final EventSubscriber subscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof DeleteDataEvent) {
                    deleteEvents.add((DeleteDataEvent) event);
                }
            }
        };
        final PartitionMap pm = new PartitionMap(eventBus, 256, clock);
        eventBus.subscribe(subscriber);

        pm.setData(input("ttl-key", "v1"));
        assertTrue(pm.getData("ttl-key").isPresent(), "Setup: ttl-key should be present");

        // EXPIRE at clock 0 with a 1000ms TTL -> deadline at epoch-millis 1000
        assertTrue(pm.expireData(new KeyDataExpireInput("ttl-key", Duration.ofMillis(1000L))),
                "EXPIRE of a live key should succeed");
        assertTrue(pm.getData("ttl-key").isPresent(), "The entry should still read before its deadline");

        clock.advanceMillis(1000L);
        assertTrue(pm.getData("ttl-key").isEmpty(),
                "Expired entry must be served as absent (checked read) at its deadline");

        // The sweep frees the memory and emits a delete event for it
        assertEquals(1, pm.sweepExpired(), "The sweep should remove exactly the one expired entry");
        assertEquals(1, deleteEvents.size(), "The sweep should emit one DeleteDataEvent");
        assertEquals("ttl-key", deleteEvents.get(0).getKey());
        assertTrue(pm.getData("ttl-key").isEmpty(), "The swept key must still read as absent");
    }

    @Test
    void testExpireOfAbsentKeyIsANoOpAtTheMapLevel() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        final PartitionMap pm = new PartitionMap(eventBus, 256, clock);

        assertFalse(pm.expireData(new KeyDataExpireInput("absent", Duration.ofMillis(1000L))),
                "EXPIRE of an absent key should report that it changed nothing");
        assertTrue(pm.getData("absent").isEmpty(), "EXPIRE of an absent key must create no entry");
    }

    @Test
    void testSweepLeavesNonExpiredEntriesAlone() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        final PartitionMap pm = new PartitionMap(eventBus, 256, clock);

        pm.setData(input("keep", "v1")); // no TTL: never expires
        pm.setData(input("drop", "v1"));
        pm.expireData(new KeyDataExpireInput("drop", Duration.ofMillis(1000L)));

        clock.advanceMillis(1500L); // "drop" is past its deadline, "keep" is unaffected
        assertEquals(1, pm.sweepExpired(), "Only the expired entry should be removed");

        assertTrue(pm.getData("keep").isPresent(),
                "TTL-less entries must survive the sweep even after the clock advanced");
        assertTrue(pm.getData("drop").isEmpty(), "The expired entry must be gone");
    }

    @Test
    void testExpiredEntryIsNeverResurrectedByASecondExpire() {
        final MutableClock clock = new MutableClock(Instant.EPOCH);
        final PartitionMap pm = new PartitionMap(eventBus, 256, clock);

        pm.setData(input("key", "v1"));
        pm.expireData(new KeyDataExpireInput("key", Duration.ofMillis(1000L)));
        clock.advanceMillis(1500L); // now past the deadline

        assertFalse(pm.expireData(new KeyDataExpireInput("key", Duration.ofMillis(10_000L))),
                "A late EXPIRE on an already-expired entry must not change it");
        assertTrue(pm.getData("key").isEmpty(),
                "A late EXPIRE must never resurrect an already-expired entry");
    }

    @Test
    void testKeyDataExpireInputRejectsNonPositiveTtls() {
        assertThrows(IllegalArgumentException.class,
                () -> new KeyDataExpireInput("key", Duration.ZERO), "A zero TTL is not a positive TTL");
        assertThrows(IllegalArgumentException.class,
                () -> new KeyDataExpireInput("key", Duration.ofSeconds(-1)), "A negative TTL is not a positive TTL");
        assertThrows(NullPointerException.class,
                () -> new KeyDataExpireInput(null, Duration.ofSeconds(1)), "The key must not be null");
        assertThrows(NullPointerException.class,
                () -> new KeyDataExpireInput("key", null), "The TTL must not be null");
    }
}