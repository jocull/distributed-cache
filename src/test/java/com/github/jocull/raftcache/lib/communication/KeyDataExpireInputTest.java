package com.github.jocull.raftcache.lib.communication;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the expire entry carrier (stage 7(a)).
 *
 * <p>Pre-fix the constructor spoke the pre-campaign style: messageless
 * {@code Objects.requireNonNull(key)} / {@code (ttl)} — NPEs with {@code null} messages, so
 * neither axis was ever named. The positivity check already spoke the house IAE family
 * ({@code "ttl must be positive. Provided: ..."}); the null axes must speak the
 * constructor-carrier family (named NPE) like the sealed siblings, and the positivity
 * refusal must survive the null-intake seal with its message intact.
 */
public class KeyDataExpireInputTest {

    @Test
    void testNullKeyNamesAxis() {
        // Pre-fix: NPE thrown, message null — the key axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new KeyDataExpireInput(null, Duration.ofSeconds(1L)),
                "A null key must be refused at the constructor");
        assertEquals("key", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testNullTtlNamesAxis() {
        // Pre-fix: NPE thrown, message null — the ttl axis never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new KeyDataExpireInput("key-1", null),
                "A null ttl must be refused at the constructor");
        assertEquals("ttl", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testZeroTtlKeepsNamedIaE() {
        // The pre-existing named IAE must survive the null-intake seal — a zero ttl is a
        // different refusal (IAE, named) than a null one (NPE, named).
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new KeyDataExpireInput("key-1", Duration.ZERO),
                "A zero ttl must keep refusing at the constructor head");
        assertEquals("ttl must be positive. Provided: PT0S", ex.getMessage());
    }

    @Test
    void testNegativeTtlKeepsNamedIaE() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new KeyDataExpireInput("key-1", Duration.ofSeconds(-1L)),
                "A negative ttl must keep refusing at the constructor head");
        assertEquals("ttl must be positive. Provided: PT-1S", ex.getMessage());
    }

    @Test
    void testValidConstructionSanity() {
        final KeyDataExpireInput input = new KeyDataExpireInput("key-1", Duration.ofSeconds(2L));
        assertEquals("key-1", input.getKey());
        assertEquals(Duration.ofSeconds(2L), input.getTtl());
    }
}
