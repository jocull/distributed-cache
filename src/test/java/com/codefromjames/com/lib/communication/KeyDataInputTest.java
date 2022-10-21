package com.codefromjames.com.lib.communication;

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
}