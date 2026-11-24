package com.github.jocull.raftcache.lib.communication;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class KeyDataInputTest {
    @Test
    void testDataIsImmutable() {
        final Map<String, byte[]> initial = new HashMap<>();
        initial.put("hello", new byte[] { 0, 1, 2, 3 });

        final KeyDataInput snapshot = new KeyDataInput("key", initial);
        assertArrayEquals(new byte[]{0, 1, 2, 3}, initial.get("hello"));
        assertEquals("key", snapshot.getKey());
        assertArrayEquals(new byte[]{0, 1, 2, 3}, snapshot.getData().get("hello"));

        initial.put("hello", new byte[] { 3, 2, 1, 0 });
        assertArrayEquals(new byte[] { 3, 2, 1, 0 }, initial.get("hello"));
        assertArrayEquals(new byte[] { 0, 1, 2, 3 }, snapshot.getData().get("hello"));

        initial.clear();
        assertNull(initial.get("hello"));
        assertArrayEquals(new byte[] { 0, 1, 2, 3 }, snapshot.getData().get("hello"));

        assertThrows(UnsupportedOperationException.class, () -> snapshot.getData().put("hello", new byte[16]));
    }

    // --- Non-happy-path intake pins (stage 7(a)): the constructor-carrier axes must be named ---

    @Test
    void testNullKeyNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the key axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new KeyDataInput(null, Map.of("value", new byte[] { 1 })),
                "A null key must be refused at the constructor");
        assertEquals("key", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testNullDataNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the data axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new KeyDataInput("key-1", null),
                "A null data map must be refused at the constructor");
        assertEquals("data", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }
}