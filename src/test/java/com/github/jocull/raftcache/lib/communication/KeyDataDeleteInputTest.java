package com.github.jocull.raftcache.lib.communication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the delete entry carrier (stage 7(a)).
 *
 * <p>Pre-fix the constructor spoke the pre-campaign style: a messageless
 * {@code Objects.requireNonNull(key)} — an NPE with a {@code null} message, so the axis (the
 * <em>key</em>) was never named and the refusal was indistinguishable from a failure deep in
 * the wiring. The house constructor-carrier family names the dependency it refuses (the sealed
 * {@code DistributedCache} constructor since 6(z) speaks {@code "clusterEventBus"},
 * {@code "nodes"}, {@code "partitionMap"}); this carrier must speak it too.
 */
public class KeyDataDeleteInputTest {

    @Test
    void testNullKeyNamesAxis() {
        // Pre-fix: an NPE is thrown (the guard sat in the right place) but its message is
        // null — the axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new KeyDataDeleteInput(null),
                "A null key must be refused at the constructor");
        assertEquals("key", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        final KeyDataDeleteInput input = new KeyDataDeleteInput("key-1");
        assertNotNull(input);
        assertEquals("key-1", input.getKey());
    }
}
