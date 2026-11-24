package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Non-happy-path intake of the PassThruMiddleware facade-intake method line (stage 8(a)).
 *
 * <p>Pre-fix the head spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(raftNode)} — an NPE with a {@code null} message, so the
 * raftNode axis was never named. The 8(a) fix moves it to the named 2-arg form in the
 * house method-intake family (the 6(z) line's prose style, the same voice as this
 * file's "nodeAddress must not be null" intakes); this was the last remaining bare
 * guard on the line.
 */
public class PassThruMiddlewareIntakeTest {

    // --- Non-happy-path intake pins (stage 8(a)): the method-intake axes must be named (prose family) ---

    @Test
    void testNullRaftNodeNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the raftNode axis was never named. Zero side effects: the refusal precedes the
        // registry write.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new PassThruMiddleware().addNode(null),
                "A null raftNode must be refused at the facade intake");
        assertEquals("raftNode must not be null", ex.getMessage(),
                "A method intake must name the dependency it refuses");
    }

    @Test
    void testValidIntakeSanity() {
        // The legitimate-intake discrimination: a well-formed node still registers and
        // reads back at its own address.
        final NodeAddress a = new NodeAddress("127.0.0.1:9100");
        final PassThruMiddleware middleware = new PassThruMiddleware();
        middleware.addNode(new FakeRaftNode("nodeA", a));
        assertTrue(middleware.getAddressRaftNodeMap().containsKey(a),
                "A well-formed node must register at its own address");
    }
}
