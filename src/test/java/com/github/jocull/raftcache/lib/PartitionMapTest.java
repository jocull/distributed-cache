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
    void testConstructorNamesItsDependencies() {
        final NullPointerException busRefusal = assertThrows(NullPointerException.class,
                () -> new PartitionMap(null, 16));
        assertEquals("eventBus must not be null", busRefusal.getMessage(),
                "A null bus must name the bus, not the JDK's parameter name");

        final NullPointerException clockRefusal = assertThrows(NullPointerException.class,
                () -> new PartitionMap(eventBus, 16, null));
        assertEquals("clock must not be null", clockRefusal.getMessage(),
                "A null clock must name the clock");
    }

    @Test
    void testGetPartitionForKeyRefusesNullKey() {
        final PartitionMap pm = new PartitionMap(eventBus, 16);
        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> pm.getPartitionForKey(null));
        assertEquals("key must not be null", refusal.getMessage(), "A null key must name the key");
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

    // ---- 6(v) intake guards (store intakes) ----
    //
    // The five store intakes (getData/setData/deleteData/expireData/restore) used to
    // dereference a null wherever one slipped past a carrier's constructor, and the
    // write paths bumped the global version *before* the dereference -- a refusal that
    // then threw leaked a consumed version into every later entry. These pins refuse at
    // the intake with a named exception, before any state change or version bump.

    /**
     * Forge a PUT carrier the {@link KeyDataInput} constructor would never produce (a
     * null key): the same construction-bypass shape as the 6(r) forges -- the carrier
     * the store intake must refuse no matter how it arrived.
     */
    private static KeyDataInput forgePutNullKey() {
        final KeyDataInput base = new KeyDataInput("key", Map.of("data", new byte[]{'v'}));
        try {
            final java.lang.reflect.Field field = KeyDataInput.class.getDeclaredField("key");
            field.setAccessible(true);
            field.set(base, null);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the null PUT key", e);
        }
    }

    /**
     * Forge a PUT carrier the {@link KeyDataInput} constructor would never produce (a
     * null data map) -- the carrier the store intake must refuse before it stamps a
     * value.
     */
    private static KeyDataInput forgePutNullData() {
        final KeyDataInput base = new KeyDataInput("key", Map.of("data", new byte[]{'v'}));
        try {
            final java.lang.reflect.Field field = KeyDataInput.class.getDeclaredField("data");
            field.setAccessible(true);
            field.set(base, null);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the null PUT data", e);
        }
    }

    /**
     * Forge an EXPIRE carrier the constructor would never produce (a null key).
     */
    private static KeyDataExpireInput forgeExpireNullKey() {
        final KeyDataExpireInput base = new KeyDataExpireInput("key", Duration.ofMillis(1000L));
        try {
            final java.lang.reflect.Field field = KeyDataExpireInput.class.getDeclaredField("key");
            field.setAccessible(true);
            field.set(base, null);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the null EXPIRE key", e);
        }
    }

    /**
     * Forge an EXPIRE carrier the constructor would never produce (a null TTL).
     */
    private static KeyDataExpireInput forgeExpireNullTtl() {
        final KeyDataExpireInput base = new KeyDataExpireInput("key", Duration.ofMillis(1000L));
        try {
            final java.lang.reflect.Field field = KeyDataExpireInput.class.getDeclaredField("ttl");
            field.setAccessible(true);
            field.set(base, null);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the null EXPIRE TTL", e);
        }
    }

    @Test
    void testStoreIntakeGuards() {
        // ---- getData / setData / deleteData (raw-string and carrier intakes) ----
        final PartitionMap pm = new PartitionMap(eventBus, 16);

        final IllegalArgumentException readRefusal = assertThrows(IllegalArgumentException.class,
                () -> pm.getData(null));
        assertEquals("key must not be null", readRefusal.getMessage(), "A null read key must name the key");

        final IllegalArgumentException setRefusal = assertThrows(IllegalArgumentException.class,
                () -> pm.setData(null));
        assertEquals("input must not be null", setRefusal.getMessage(), "A null PUT carrier must name the carrier");

        final IllegalArgumentException deleteRefusal = assertThrows(IllegalArgumentException.class,
                () -> pm.deleteData((String) null));
        assertEquals("key must not be null", deleteRefusal.getMessage(), "A null DELETE key must name the key");

        // The construction-bypassed carriers (null key / null data) are refused too
        final IllegalArgumentException bypassKeyRefusal = assertThrows(IllegalArgumentException.class,
                () -> pm.setData(forgePutNullKey()));
        assertEquals("key must not be null", bypassKeyRefusal.getMessage(), "A nulled PUT key must name the key");
        final IllegalArgumentException bypassDataRefusal = assertThrows(IllegalArgumentException.class,
                () -> pm.setData(forgePutNullData()));
        assertEquals("data must not be null", bypassDataRefusal.getMessage(), "A nulled PUT data must name the data");

        // Refusals must not consume a version: the four refusals above each bumped the
        // global counter before they threw (pre-guard the legitimate SET landed on
        // version 5); post-guard it still gets the *first* global version
        pm.setData(input("key", "v1"));
        assertEquals(1L, pm.getData("key").orElseThrow().getVersion(),
                "A refused intake must not leak a consumed version into a later legitimate write");

        // ---- expireData (carrier intake, both axes) ----
        final PartitionMap ttlMap = new PartitionMap(eventBus, 16, new MutableClock(Instant.EPOCH));

        final IllegalArgumentException expireRefusal = assertThrows(IllegalArgumentException.class,
                () -> ttlMap.expireData(null));
        assertEquals("input must not be null", expireRefusal.getMessage(),
                "A null EXPIRE carrier must name the carrier");
        final IllegalArgumentException expireKeyRefusal = assertThrows(IllegalArgumentException.class,
                () -> ttlMap.expireData(forgeExpireNullKey()));
        assertEquals("key must not be null", expireKeyRefusal.getMessage(), "A nulled EXPIRE key must name the key");
        final IllegalArgumentException expireTtlRefusal = assertThrows(IllegalArgumentException.class,
                () -> ttlMap.expireData(forgeExpireNullTtl()));
        assertEquals("ttl must not be null", expireTtlRefusal.getMessage(), "A nulled EXPIRE TTL must name the TTL");

        // Version-leak pin on the TTL path: SET -> legitimate EXPIRE. Pre-guard the three
        // EXPIRE refusals above each consumed a version (the live entry landed on version
        // 5); post-guard the refusal leaks nothing and the live entry stays on version 2
        ttlMap.setData(input("key", "v1"));
        ttlMap.expireData(new KeyDataExpireInput("key", Duration.ofMillis(1000L)));
        assertEquals(2L, ttlMap.getData("key").orElseThrow().getVersion(),
                "A refused EXPIRE must not leak a consumed version into a later legitimate write");

        // ---- restore (the snapshot-reinstall intake) ----
        final PartitionMap snapshotMap = new PartitionMap(eventBus, 16);
        snapshotMap.setData(input("existing", "v1"));

        final IllegalArgumentException restoreNullRefusal = assertThrows(IllegalArgumentException.class,
                () -> snapshotMap.restore(null));
        assertEquals("state must not be null", restoreNullRefusal.getMessage(), "A null state must name the state");

        // Construction-bypassed state (a plain HashMap carrying a null key / a null
        // DataVersion -- shapes neither Map.of nor a captured state would produce) is
        // refused too
        final Map<String, DataVersion> nullKeyState = new HashMap<>();
        nullKeyState.put(null, new DataVersion(Map.of("data", "v".getBytes(StandardCharsets.UTF_8)), 1L));
        final IllegalArgumentException restoreKeyRefusal = assertThrows(IllegalArgumentException.class,
                () -> snapshotMap.restore(nullKeyState));
        assertEquals("key must not be null", restoreKeyRefusal.getMessage(), "A nulled restore key must name the key");

        final Map<String, DataVersion> nullValueState = new HashMap<>();
        nullValueState.put("key", null);
        final IllegalArgumentException restoreValueRefusal = assertThrows(IllegalArgumentException.class,
                () -> snapshotMap.restore(nullValueState));
        assertEquals("state value must not be null", restoreValueRefusal.getMessage(),
                "A nulled restore value must name the value");

        // Refusals must not disturb the existing state at all (pre-guard the NPEs were
        // already before any partition mutation; this pins that)
        assertTrue(snapshotMap.getData("existing").isPresent(),
                "A refused restore must not drop the existing state");
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8),
                snapshotMap.getData("existing").orElseThrow().getData().get("data"));
        assertTrue(snapshotMap.getData("key").isEmpty(),
                "A refused restore must not install partial state");

        // And the intake remains fully usable: a legitimate restore replaces the state,
        // fast-forwarding the version counter for subsequent local writes
        final Map<String, DataVersion> state = new HashMap<>();
        state.put("restored", new DataVersion(Map.of("data", "v2".getBytes(StandardCharsets.UTF_8)), 7L));
        snapshotMap.restore(state);
        assertTrue(snapshotMap.getData("restored").isPresent(),
                "A legitimate restore must install its state");
        assertEquals(7L, snapshotMap.getData("restored").orElseThrow().getVersion(),
                "The restored entry must carry its restored version");
        assertTrue(snapshotMap.getData("existing").isEmpty(),
                "Restore replaces the state: keys absent from it are dropped");
        snapshotMap.setData(input("after", "v1"));
        assertEquals(8L, snapshotMap.getData("after").orElseThrow().getVersion(),
                "After a fast-forwarding restore, the next local write must still be strictly newer");
    }
}