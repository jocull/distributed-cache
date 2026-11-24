package com.github.jocull.raftcache.lib.raft.middleware;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Non-happy-path intake of the middleware's bind-host axis (stage 7(c)).
 *
 * <p>Pre-fix the host constructor spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(host)} — an NPE with a {@code null} message, so the host
 * axis was never named. The 7(c) fix moves the head to the named 2-arg form in the house
 * constructor-carrier family; the no-arg constructor delegates here with the loopback
 * default, so a refused null died before the bind with no axis named — indistinguishable
 * from a failure deep in the channel wiring.
 */
public class SocketChannelMiddlewareConstructionTest {

    // --- Non-happy-path intake pins (stage 7(c)): the constructor-carrier axes must be named ---

    @Test
    void testNullHostNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the host axis was never named. Zero side effects: the refusal precedes the bind,
        // so no socket exists and no acceptor thread starts.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new SocketChannelMiddleware(null),
                "A null host must be refused at the constructor");
        assertEquals("host", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed host still binds and its
        // wire address still reads back at the loopback prefix.
        try (final SocketChannelMiddleware middleware = new SocketChannelMiddleware("127.0.0.1")) {
            assertTrue(middleware.getAddress().getAddress().startsWith("127.0.0.1:"),
                    "The bound address must be the loopback host + an ephemeral port: "
                            + middleware.getAddress());
        }
    }
}
