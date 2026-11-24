package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.EventSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class KeyMapTest {
    private EventBus eventBus;
    private KeyMap keyMap;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
        keyMap = new KeyMap(new PartitionMap(eventBus, 4), new Partition(0));
    }

    @Test
    void testSetEntryRefusesNullKey() {
        final List<Object> events = new ArrayList<>();
        eventBus.subscribe(new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                events.add(event);
            }
        });

        final DataVersion version = new DataVersion(Map.of("data", new byte[]{1}), 1L);
        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.setEntry(null, version));
        assertEquals("key must not be null", refusal.getMessage(), "A null install key must name the key");

        // Zero-state-change discriminators: a refused install must leave neither a phantom
        // entry (pre-fix a null key was a legal HashMap key -- installed and served under
        // it) nor a published event naming an unnameable key
        assertTrue(keyMap.capture().isEmpty(), "A refused install must not leave a phantom entry");
        assertTrue(events.isEmpty(), "A refused install must not publish an event under a null key");
    }

    @Test
    void testSetEntryRefusesNullVersion() {
        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.setEntry("key", null));
        assertEquals("data must not be null", refusal.getMessage(), "A nulled version must name the data");

        // Zero-state-change discriminator: no KeyValue shell may be left behind -- pre-fix
        // the shell was installed BEFORE the deep null-version throw, so deleteEntry could
        // remove it (publishing a DeleteDataEvent) for a key that was never actually written
        assertFalse(keyMap.deleteEntry("key", 1L),
                "A refused write must not leave a shell deleteEntry could remove");
    }

    @Test
    void testGetEntryRefusesNullKey() {
        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.getEntry(null));
        assertEquals("key must not be null", refusal.getMessage(), "A null read key must name the key");
    }

    @Test
    void testDeleteEntryRefusesNullKey() {
        final List<Object> events = new ArrayList<>();
        eventBus.subscribe(new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                events.add(event);
            }
        });

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.deleteEntry(null, 1L));
        assertEquals("key must not be null", refusal.getMessage(), "A null delete key must name the key");

        assertTrue(events.isEmpty(), "A refused delete must not publish an event under a null key");
    }

    @Test
    void testExpireEntryRefusesNullKey() {
        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.expireEntry(null, 1L, 0L, 1000L));
        assertEquals("key must not be null", refusal.getMessage(), "A null EXPIRE key must name the key");
    }

    @Test
    void testRestoreRefusesNullState() {
        keyMap.setEntry("existing", new DataVersion(Map.of("data", new byte[]{1}), 1L));

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.restore(null));
        assertEquals("state must not be null", refusal.getMessage(), "A null state must name the state");

        // A refused replace must not have dropped the installed state
        assertTrue(keyMap.getEntry("existing").isPresent(),
                "A refused replace must not drop the installed state");
    }

    @Test
    void testRestoreRefusesNullKey() {
        keyMap.setEntry("existing", new DataVersion(Map.of("data", new byte[]{1}), 1L));
        final Map<String, DataVersion> forged = new HashMap<>();
        forged.put(null, new DataVersion(Map.of("data", new byte[]{2}), 1L));

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.restore(forged));
        assertEquals("key must not be null", refusal.getMessage(), "A nulled restore key must name the key");

        // Zero-state-change discriminator: the partition must hold EXACTLY what it held
        // before -- pre-fix the partition was cleared first and the null-keyed phantom
        // installed alongside the throw, the legitimate key dropped and the phantom
        // reachable by reads and deletes
        final Map<String, DataVersion> survivor = keyMap.capture();
        assertEquals(1, survivor.size(), "A refused replace must neither install nor drop entries");
        assertTrue(keyMap.getEntry("existing").isPresent(), "A refused replace must not drop the installed state");
        assertArrayEquals(new byte[]{1}, survivor.get("existing").getData().get("data"));
    }

    @Test
    void testRestoreRefusesNullValue() {
        keyMap.setEntry("existing", new DataVersion(Map.of("data", new byte[]{1}), 1L));
        final Map<String, DataVersion> forged = new HashMap<>();
        forged.put("installed", new DataVersion(Map.of("data", new byte[]{2}), 1L));
        forged.put("null-valued", null);

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyMap.restore(forged));
        assertEquals("state value must not be null", refusal.getMessage(), "A nulled restore value must name the value");

        // Same zero-state-change shape: no partial install, no drop of the installed state
        final Map<String, DataVersion> survivor = keyMap.capture();
        assertEquals(1, survivor.size(), "A refused replace must neither install nor drop entries");
        assertTrue(keyMap.getEntry("existing").isPresent(), "A refused replace must not drop the installed state");
        assertTrue(keyMap.getEntry("installed").isEmpty(),
                "A refused replace must not install partial state");
    }
}
