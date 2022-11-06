package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TermIndexTest {
    @Test
    void testConstructorGuards() {
        assertThrows(IllegalArgumentException.class, () -> new TermIndex(-1, 99));
        assertThrows(IllegalArgumentException.class, () -> new TermIndex(0, -1));
        assertThrows(IllegalArgumentException.class, () -> new TermIndex(-1, -1));
        assertDoesNotThrow(() -> new TermIndex(0, 0));
    }

    @Test
    void testIncrement() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = a.incrementIndex();

        assertEquals(1, b.getTerm());
        assertEquals(2L, b.getIndex());
    }

    @Test
    void testEquals() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = new TermIndex(1, 1);
        final TermIndex c = new TermIndex(1, 2);
        final TermIndex d = new TermIndex(2, 1);

        assertEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(a, d);

        assertNotEquals(b, c);
        assertNotEquals(b, d);

        assertNotEquals(c, d);
    }

    @Test
    void testHashing() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = new TermIndex(1, 1);
        final TermIndex c = new TermIndex(1, 2);
        final TermIndex d = new TermIndex(2, 1);

        final Map<TermIndex, TermIndex> map = new HashMap<>();
        map.put(a, a);
        map.put(b, b); // Dupe!
        map.put(c, c);
        map.put(d, d);
        assertEquals(3, map.size());

        final TermIndex a2 = map.get(new TermIndex(1, 1));
        final TermIndex b2 = map.get(new TermIndex(1, 1)); // Dupe!
        final TermIndex c2 = map.get(new TermIndex(1, 2));
        final TermIndex d2 = map.get(new TermIndex(2, 1));

        assertSame(b, a2); // Replaced by `b` on insert
        assertSame(b, b2); // Dupe!
        assertSame(c, c2);
        assertSame(d, d2);
    }

    @SuppressWarnings("EqualsWithItself")
    @Test
    void testComparison() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = new TermIndex(1, 1);
        final TermIndex c = new TermIndex(1, 2);
        final TermIndex d = new TermIndex(2, 1);

        assertEquals(0, a.compareTo(a));
        assertEquals(0, a.compareTo(b));
        assertEquals(-1, a.compareTo(c));
        assertEquals(-1, a.compareTo(d));

        assertEquals(0, b.compareTo(a));
        assertEquals(0, b.compareTo(b));
        assertEquals(-1, b.compareTo(c));
        assertEquals(-1, b.compareTo(d));

        assertEquals(1, c.compareTo(a));
        assertEquals(1, c.compareTo(b));
        assertEquals(0, c.compareTo(c));
        assertEquals(-1, c.compareTo(d));

        assertEquals(1, d.compareTo(a));
        assertEquals(1, d.compareTo(b));
        assertEquals(1, d.compareTo(c));
        assertEquals(0, d.compareTo(d));

        //noinspection ResultOfMethodCallIgnored
        assertThrows(NullPointerException.class, () -> a.compareTo(null));
    }

    @Test
    void testBeforeAndAfter() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = new TermIndex(1, 1);
        final TermIndex c = new TermIndex(1, 2);
        final TermIndex d = new TermIndex(2, 1);

        assertFalse(a.isDirectlyBefore(a));
        assertFalse(a.isDirectlyAfter(a));

        assertTrue(a.isDirectlyBefore(c));
        assertFalse(a.isDirectlyAfter(c));
        assertFalse(c.isDirectlyBefore(a));
        assertTrue(c.isDirectlyAfter(a));

        assertFalse(c.isDirectlyBefore(d));
        assertFalse(c.isDirectlyAfter(d));

        assertFalse(d.isDirectlyBefore(c));
        assertFalse(d.isDirectlyAfter(c));
    }

    @Test
    void testReplacement() {
        final TermIndex a = new TermIndex(1, 1);
        final TermIndex b = new TermIndex(1, 1);
        final TermIndex c = new TermIndex(1, 2);
        final TermIndex d = new TermIndex(2, 1);

        assertFalse(a.isReplacementOf(a));
        assertFalse(a.isReplacementOf(b));
        assertFalse(a.isReplacementOf(c));
        assertFalse(a.isReplacementOf(d));

        assertFalse(b.isReplacementOf(a));
        assertFalse(b.isReplacementOf(b));
        assertFalse(b.isReplacementOf(c));
        assertFalse(b.isReplacementOf(d));

        assertFalse(c.isReplacementOf(a));
        assertFalse(c.isReplacementOf(b));
        assertFalse(c.isReplacementOf(c));
        assertFalse(c.isReplacementOf(d));

        assertTrue(d.isReplacementOf(a));
        assertTrue(d.isReplacementOf(b));
        assertTrue(d.isReplacementOf(c));
        assertFalse(d.isReplacementOf(d));
    }
}