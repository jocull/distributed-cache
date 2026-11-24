package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.event.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class KeyValueTest {
    private EventBus eventBus;
    private KeyMap keyMap;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
        keyMap = new KeyMap(new PartitionMap(eventBus, 4), new Partition(0));
    }

    @Test
    void testSetDataRefusesNullVersion() {
        final KeyValue keyValue = new KeyValue("key", keyMap);
        keyValue.setData(new DataVersion(Map.of("data", new byte[]{1}), 1L));

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyValue.setData(null));
        assertEquals("data must not be null", refusal.getMessage(), "A null version must name the version");

        // Zero-state-change discriminator: a refused write must not erase the value it
        // already held -- pre-fix the null was STORED first and only then the NPE fired
        // at data.getData(), leaving the key masquerading as never written (getData()
        // served Optional.empty)
        final DataVersion survivor = keyValue.getData().orElseThrow();
        assertEquals(1L, survivor.getVersion(), "A refused write must not consume the stored value");
        assertArrayEquals(new byte[]{1}, survivor.getData().get("data"));
    }

    @Test
    void testUpdateExpirationRefusesNullVersion() {
        final KeyValue keyValue = new KeyValue("key", keyMap);
        keyValue.setData(new DataVersion(Map.of("data", new byte[]{1}), 1L));

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyValue.updateExpiration(null));
        assertEquals("data must not be null", refusal.getMessage(), "A null version must name the version");

        // Pre-fix the null was stored silently (no exception at all) -- the key then
        // masqueraded as never written. A refusal must leave the value intact instead.
        final DataVersion survivor = keyValue.getData().orElseThrow();
        assertEquals(1L, survivor.getVersion(), "A refused expiration update must not consume the stored value");
        assertArrayEquals(new byte[]{1}, survivor.getData().get("data"));
    }

    @Test
    void testRestoreRefusesNullVersion() {
        final KeyValue keyValue = new KeyValue("key", keyMap);
        keyValue.setData(new DataVersion(Map.of("data", new byte[]{1}), 1L));

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> keyValue.restore(null));
        assertEquals("data must not be null", refusal.getMessage(), "A null version must name the version");

        // Same silent-null-store shape on the snapshot-adopt path
        final DataVersion survivor = keyValue.getData().orElseThrow();
        assertEquals(1L, survivor.getVersion(), "A refused restore must not consume the stored value");
        assertArrayEquals(new byte[]{1}, survivor.getData().get("data"));
    }
}
