package com.github.jocull.raftcache.lib.topology;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the node address carrier (stage 7(b)).
 *
 * <p>Pre-fix the constructor spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(address)} — an NPE with a {@code null} message, so the
 * address axis was never named. The 7(b) fix moves it to the named 2-arg form in the
 * house constructor-carrier family, the same seal the entry-payload family received at
 * 7(a); this carrier is the identity every sealed seam (topology, bootstrap, the
 * {@code AnnounceClusterTopology} list the 6(i) receiver refuses) rides on.
 */
public class NodeAddressTest {

    // --- Non-happy-path intake pins (stage 7(b)): the constructor-carrier axis must be named ---

    @Test
    void testNullAddressNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the address axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new NodeAddress(null),
                "A null address must be refused at the constructor");
        assertEquals("address", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed address still builds and
        // its getter still reads back (a guard that refuses everything is as broken as
        // one that refuses nothing).
        final NodeAddress address = new NodeAddress("127.0.0.1:8095");
        assertEquals("127.0.0.1:8095", address.getAddress());
        assertEquals(address, new NodeAddress("127.0.0.1:8095"));
    }
}
