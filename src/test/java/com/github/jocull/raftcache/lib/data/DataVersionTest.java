package com.github.jocull.raftcache.lib.data;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DataVersionTest {
    @Test
    void testNullDataMapIsRefused() {
        // 2-arg carrier: a null map must be refused naming the axis -- pre-fix the
        // JDK-enriched NPE from Collections.unmodifiableMap(null) named the JDK's own
        // parameter, indistinguishable from a failure deeper in the wiring
        final NullPointerException refusal = assertThrows(NullPointerException.class,
                () -> new DataVersion(null, 1L));
        assertEquals("data must not be null", refusal.getMessage(), "A null data map must name the data map");

        final NullPointerException fullRefusal = assertThrows(NullPointerException.class,
                () -> new DataVersion(null, 1L, 1_700_000_000_000L));
        assertEquals("data must not be null", fullRefusal.getMessage(), "A null data map must name the data map");
    }

    @Test
    void testDataMapIsADetachedCopy() {
        // The constructor contract (its own javadoc): the stored map is a detached copy --
        // a producer-held map must not stay mutable after construction. The DataVersion
        // is the entry payload of a Snapshot and of a key's stored value, so a producer
        // rewrite would silently mutate installed state.
        final HashMap<String, byte[]> initial = new HashMap<>();
        initial.put("hello", new byte[]{0, 1, 2, 3});

        final DataVersion snapshot = new DataVersion(initial, 1L);
        assertArrayEquals(new byte[]{0, 1, 2, 3}, snapshot.getData().get("hello"));

        initial.put("hello", new byte[]{3, 2, 1, 0});
        initial.put("spoofed", new byte[]{9});
        initial.clear();

        assertArrayEquals(new byte[]{0, 1, 2, 3}, snapshot.getData().get("hello"),
                "Producer-side mutation after construction must not rewrite the stored value");
        assertNull(snapshot.getData().get("spoofed"),
                "Producer-side additions after construction must not appear in the stored value");
    }

    @Test
    void testNullKeyIsRefused() {
        // A construction-bypassed / deserialized carrier can hold a null key (HashMap
        // permits it) -- the Map.copyOf-based carriers (KeyDataInput/KeyDataOutput) refuse
        // such a map; the DataVersion constructor must too, not carry it into a snapshot
        // entry or a stored value
        final HashMap<String, byte[]> state = new HashMap<>();
        state.put(null, new byte[]{1});
        assertThrows(NullPointerException.class, () -> new DataVersion(state, 1L),
                "A null key must be refused at the constructor");
    }

    @Test
    void testNullValueIsRefused() {
        // Same shape on the value axis: a map with a null value must be refused
        final HashMap<String, byte[]> state = new HashMap<>();
        state.put("data", null);
        assertThrows(NullPointerException.class, () -> new DataVersion(state, 1L),
                "A null value must be refused at the constructor");
    }

    @Test
    void testStoredMapIsNotMutable() {
        final DataVersion version = new DataVersion(Map.of("data", new byte[]{1}), 1L);
        assertThrows(UnsupportedOperationException.class,
                () -> version.getData().put("spoofed", new byte[]{0}),
                "The stored map must be immutable");
    }
}
