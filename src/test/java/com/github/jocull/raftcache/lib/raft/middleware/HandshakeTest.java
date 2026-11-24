package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the handshake frame carrier (stage 7(b)).
 *
 * <p>Pre-fix the constructor spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(client)} / {@code (target)} — NPEs with {@code null}
 * messages, so neither axis was ever named. The 7(b) fix moves both heads to the named
 * 2-arg form in the house constructor-carrier family, the same seal the entry-payload
 * family received at 7(a); this carrier is the first frame every connection writes (the
 * N1 bootstrap handshake), so a refused null was indistinguishable from a failure deep
 * in the channel wiring.
 */
public class HandshakeTest {

    // --- Non-happy-path intake pins (stage 7(b)): the constructor-carrier axes must be named ---

    @Test
    void testNullClientNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the client axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new Handshake(null, new NodeAddress("target-addr")),
                "A null client must be refused at the constructor");
        assertEquals("client", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testNullTargetNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the target axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new Handshake(new NodeAddress("client-addr"), null),
                "A null target must be refused at the constructor");
        assertEquals("target", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed frame still builds and
        // its getters still read back.
        final Handshake handshake = new Handshake(new NodeAddress("client-addr"), new NodeAddress("target-addr"));
        assertEquals(new NodeAddress("client-addr"), handshake.getClient());
        assertEquals(new NodeAddress("target-addr"), handshake.getTarget());
    }
}
