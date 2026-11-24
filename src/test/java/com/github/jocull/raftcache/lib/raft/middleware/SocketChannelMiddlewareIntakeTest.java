package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Non-happy-path intake of the SocketChannelMiddleware facade-intake method line (stage 8(a)).
 *
 * <p>Pre-fix the three heads spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(raftNode)} / {@code (source.getNodeAddress())} /
 * {@code (newReceiver)} — NPEs with {@code null} messages, so the axes were never named.
 * The 8(a) fix moves them to the named 2-arg form in the house method-intake family (the
 * 6(z) line's prose style, the same voice as this file's "source must not be null" /
 * "nodeAddress must not be null" intakes). The addNode and openChannel null-source
 * refusals were already class-pinned pre-campaign (message-agnostic, so unaffected), but
 * none of the three axes was named until this round.
 */
public class SocketChannelMiddlewareIntakeTest {

    // --- Non-happy-path intake pins (stage 8(a)): the method-intake axes must be named (prose family) ---

    @Test
    void testNullRaftNodeNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the raftNode axis was never named. Zero side effects: the refusal precedes the
        // registry write.
        final SocketChannelMiddleware middleware = new SocketChannelMiddleware();
        try {
            final NullPointerException ex = assertThrows(NullPointerException.class,
                    () -> middleware.addNode(null),
                    "A null raftNode must be refused at the facade intake");
            assertEquals("raftNode must not be null", ex.getMessage(),
                    "A method intake must name the dependency it refuses");
        } finally {
            middleware.close();
        }
    }

    @Test
    void testNullClientAddressNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the axis was never named. A real node
        // can never report a null address (NodeAddress's own intake forbids it), so the
        // refusal is pinned via a node whose getNodeAddress() answers null; the refusal
        // precedes the dial, so no socket is opened.
        final SocketChannelMiddleware middleware = new SocketChannelMiddleware();
        try {
            final RaftNode nodeWithNullAddress = mock(RaftNode.class); // answers null for getNodeAddress()
            final NullPointerException ex = assertThrows(NullPointerException.class,
                    () -> middleware.openChannel(nodeWithNullAddress, middleware.getAddress()),
                    "A source whose node address is null must be refused before any dial");
            assertEquals("source.getNodeAddress() must not be null", ex.getMessage(),
                    "A method intake must name the dependency it refuses");
        } finally {
            middleware.close();
        }
    }

    @Test
    void testNullReceiverNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the receiver axis was never named.
        // Receiver install is a public ChannelSide intake (Connection.setReceiver is
        // private); refusal must happen at the intake, not inside delivery.
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", mwB.getAddress());
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);

            final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, mwB.getAddress());
            final NullPointerException ex = assertThrows(NullPointerException.class,
                    () -> sideAB.setReceiver(null),
                    "A null receiver must be refused at the channel-side intake");
            assertEquals("newReceiver must not be null", ex.getMessage(),
                    "A method intake must name the dependency it refuses");
        }
    }

    @Test
    void testValidIntakeSanity() {
        // The legitimate-intake discrimination: a well-formed node still registers, its
        // channel still opens, and the receiver still installs.
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", mwB.getAddress());
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);

            assertTrue(mwA.getRegisteredNodes().containsKey(nodeA.getNodeAddress()),
                    "A well-formed node must register at its own address");
            final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, mwB.getAddress());
            sideAB.setReceiver(nodeA::record);
            assertEquals(mwB.getAddress(), sideAB.getAddress(),
                    "A well-formed channel must open and install its receiver");
        }
    }
}
