package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.messages.TermIndex;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/*
 * Transport-level tests of SocketChannelMiddleware over real 127.0.0.1 sockets, mirroring the
 * behaviors PassThruMiddleware provides (the parity bar for phase 3, NETWORKING.md 3):
 *
 *   N1  handshake: openChannel returns only after the peer's receiver is installed, so
 *                 both ends can immediately talk; the side reports the REMOTE node's address
 *   N2  strict per-channel FIFO delivery over the socket
 *   N3  interrupt(node)/restore(node): outbound drops at the door, inbound frames are
 *                 consumed but discarded (the stream never desynchronizes), traffic resumes
 *                 in order
 *
 * The fake node (FakeRaftNode) plays only the server-side role the real RaftNodeImpl plays
 * on the accepted side: acceptConnection installs a receiver sink. Everything else is inert.
 */
class SocketChannelMiddlewareTest {

    /*
     * N1: the server installs the peer's receiver before it writes the ACK, so the instant
     * openChannel returns on the client side, nodeB's receiver already exists (asserted
     * directly) and traffic flows both ways after we install the client side's receiver the
     * way NodeConnectionOutboundImpl does.
     */
    @Test
    void handshakeCompletesBeforeOpenReturnsThenEchoesBothWays() {
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final NodeAddress a = mwA.getAddress();
            final NodeAddress b = mwB.getAddress();
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", a);
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", b);
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);

            // The side reports the REMOTE node's address (the getRemoteNodeAddress contract).
            final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, b);
            assertEquals(b, sideAB.getAddress(), "ChannelSide must report the remote node's address");

            // N1 core: nodeB's receiver (installed in acceptConnection) exists BEFORE this
            // call returned -- no message sent from here can outrun its sink.
            assertEquals(1, nodeB.getAcceptedSides().size(),
                    "Peer's receiver should be installed before openChannel returns");

            sideAB.setReceiver(nodeA::record); // client side: the node installs its own sink
            final ChannelMiddleware.ChannelSide sideBA = mwB.openChannel(nodeB, a);
            assertEquals(a, sideBA.getAddress(), "ChannelSide must report the remote node's address");
            sideBA.setReceiver(nodeB::record);

            // The transport now carries a real wire frame (the codec closes the frame set at
            // the 14 wire types), so the synthetic payload is a VoteResponse whose `term`
            // doubles as its delivery tag, asserted by count + term below.
            sideAB.send(frame(1));
            awaitFrames(() -> nodeB.received(), new int[]{1}, "a->b echo");

            sideBA.send(frame(2));
            awaitFrames(() -> nodeA.received(), new int[]{2}, "b->a echo");
        }
    }

    /*
     * N2: 1000 frames sent in a loop arrive strictly in send order -- the single reader per
     * connection plus the serialized send path must keep it that way over a real socket.
     */
    @Test
    void deliveryIsStrictlyFifoInOrder() {
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", mwB.getAddress());
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);

            final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, mwB.getAddress());
            sideAB.setReceiver(nodeA::record);

            final int count = 1000;
            final int[] terms = new int[count];
            for (int i = 0; i < count; i++) {
                terms[i] = i;
                sideAB.send(frame(i));
            }
            awaitFrames(() -> nodeB.received(), terms, "a->b ordered burst of " + count);
        }
    }

    /*
     * N3: interrupt(A) seals node A alone. A's outbound sends drop at the door (nothing hits
     * the wire); B's outbound still reaches the wire, and A's reader must CONSUME the frame
     * and discard the payload -- not drop bytes -- so the stream stays framed. After
     * restore, both directions deliver the post-restore burst in strict order with the
     * discarded frames absent (a desynchronized reader would corrupt or lose "a-1"/"b-1").
     */
    @Test
    void interruptSealsNodeAndResumeKeepsFifoWithoutDesync() {
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final NodeAddress a = mwA.getAddress();
            final NodeAddress b = mwB.getAddress();
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", a);
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", b);
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);

            final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, b);
            sideAB.setReceiver(nodeA::record);
            final ChannelMiddleware.ChannelSide sideBA = mwB.openChannel(nodeB, a);
            sideBA.setReceiver(nodeB::record);

            // Baseline: both directions deliver.
            sideAB.send(frame(0));
            awaitFrames(() -> nodeB.received(), new int[]{0}, "baseline a->b");
            sideBA.send(frame(10));
            awaitFrames(() -> nodeA.received(), new int[]{10}, "baseline b->a");

            mwA.interrupt(a);
            assertTrue(mwA.isInterrupted(a), "isInterrupted should report the seal");

            // Sealed A: its own outbound drops at the door...
            sideAB.send(frame(100));
            // ...while B's outbound still crosses the wire and is consumed-but-discarded by A.
            sideBA.send(frame(100));
            // Let the single "b-loss" frame be read and discarded before we resume (loopback
            // delivery is immediate and A's reader is a dedicated thread blocked in readFrame).
            awaitMillis(500);

            // Nothing new arrived at either node: A's outbound dropped at the door, B's
            // consumed-but-discarded. Asserted as counts (the sealed payload is discarded).
            assertEquals(1, nodeB.received().size(), "a->b send while A is sealed must drop at the door");
            assertEquals(1, nodeA.received().size(), "b->a send must be consumed but discarded while A is sealed");

            // Resume: the post-seal burst still lands, strictly in order, with no trace of the
            // discarded frames -- the reader stayed in sync on the byte stream.
            mwA.restore(a);
            assertFalse(mwA.isInterrupted(a), "restore should clear the seal");

            sideAB.send(frame(1));
            sideAB.send(frame(2));
            awaitFrames(() -> nodeB.received(), new int[]{0, 1, 2}, "a->b after restore");

            sideBA.send(frame(11));
            sideBA.send(frame(12));
            awaitFrames(() -> nodeA.received(), new int[]{10, 11, 12},
                    "b->a after restore (b-loss consumed, not desyncing the stream)");
        }
    }

    /*
     * close(): idempotent, and afterwards the middleware refuses new channels while sends on
     * existing (dead) sides are silently dropped -- the same "no exception out of transport"
     * posture as a dead PassThru hop.
     */
    @Test
    void closeIsIdempotentAndDropsTraffic() {
        final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
        final SocketChannelMiddleware mwB = new SocketChannelMiddleware();
        final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
        final FakeRaftNode nodeB = new FakeRaftNode("nodeB", mwB.getAddress());
        mwA.addNode(nodeA);
        mwB.addNode(nodeB);

        final ChannelMiddleware.ChannelSide sideAB = mwA.openChannel(nodeA, mwB.getAddress());
        sideAB.setReceiver(nodeA::record);
        sideAB.send(frame(1));
        awaitFrames(() -> nodeB.received(), new int[]{1}, "pre-close a->b");

        mwA.close();
        mwA.close(); // idempotent

        // Dead side: send is dropped, never throws (asserted as a count -- the payload of a
        // dropped send never reaches the peer).
        sideAB.send(frame(2));
        assertEquals(1, nodeB.received().size(), "send on a closed channel must be a no-op");

        // New channels are refused outright.
        assertThrows(IllegalStateException.class,
                () -> mwA.openChannel(nodeA, mwB.getAddress()),
                "openChannel on a closed middleware must fail");

        // A middleware with no channels closes cleanly, twice.
        final SocketChannelMiddleware empty = new SocketChannelMiddleware();
        empty.close();
        empty.close();
    }

    /*
     * Dialing a live server socket whose registry no longer knows the handshake target: the
     * server closes the connection and the client fails the handshake with a clear error
     * (the wire is still framed -- the server never sends a half-ACK).
     */
    @Test
    void openChannelForUnregisteredTargetFails() {
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware();
             final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
            final FakeRaftNode nodeB = new FakeRaftNode("nodeB", mwB.getAddress());
            mwA.addNode(nodeA);
            mwB.addNode(nodeB);
            assertTrue(mwB.removeNode(nodeB), "registry should report removal of a known node");

            final IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> mwA.openChannel(nodeA, mwB.getAddress()),
                    "Handshake for an unregistered target must fail");
            assertTrue(error.getMessage().contains("Handshake"),
                    "Failure should name the failed handshake, was: " + error.getMessage());
            assertEquals(0, nodeB.getAcceptedSides().size(), "No side may be accepted for an unknown target");
        }
    }

    /*
     * Registry/address contract surface (parity with PassThruMiddleware): duplicate addNode
     * is rejected, malformed node addresses are rejected up front, nulls fail fast.
     */
    @Test
    void registryAndAddressContract() {
        try (final SocketChannelMiddleware mwA = new SocketChannelMiddleware()) {
            final FakeRaftNode nodeA = new FakeRaftNode("nodeA", mwA.getAddress());
            mwA.addNode(nodeA);
            assertTrue(mwA.getRegisteredNodes().containsKey(nodeA.getNodeAddress()));

            assertThrows(IllegalArgumentException.class, () -> mwA.addNode(nodeA),
                    "Adding the same node twice must be rejected");

            assertThrows(IllegalArgumentException.class,
                    () -> mwA.openChannel(nodeA, new NodeAddress("host-only-no-port")),
                    "A node address without host:port must be rejected before any dial");

            assertThrows(NullPointerException.class, () -> mwA.openChannel((RaftNode) null, nodeA.getNodeAddress()),
                    "Null source node must be rejected");

            assertThrows(NullPointerException.class, () -> mwA.openChannel((NodeAddress) null, nodeA.getNodeAddress()),
                    "Null client address must be rejected");

            // Two middlewares bind distinct ephemeral ports on the same loopback host.
            try (final SocketChannelMiddleware mwB = new SocketChannelMiddleware()) {
                assertTrue(mwA.getAddress().getAddress().startsWith("127.0.0.1:"),
                        "Address should be the bound loopback address: " + mwA.getAddress());
                assertFalse(mwA.getAddress().equals(mwB.getAddress()), "Ephemeral ports must differ");
            }
        }
    }

    // -----------------------------------------------------------------------

    /* A synthetic wire frame. The codec closes the frame set at the 14 wire types, so
     * transport tests must send a real wire message; VoteResponse's `term` field gives each
     * frame a content-level delivery tag that survives the round-trip (the POJOs carry no
     * value equality to assert against). */
    private static VoteResponse frame(int term) {
        return new VoteResponse(term, new TermIndex(term, 0), true);
    }

    /* Waits until the receiver holds exactly expectedTerms.length frames, then asserts each
     * round-tripped to a VoteResponse carrying its expected term at its position -- strict
     * per-channel FIFO with content check. */
    private static void awaitFrames(Supplier<List<Object>> received, int[] expectedTerms, String what) {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            final List<Object> got = received.get();
            if (got.size() == expectedTerms.length) {
                for (int i = 0; i < expectedTerms.length; i++) {
                    assertTrue(got.get(i) instanceof VoteResponse,
                            what + " — frame " + i + " must round-trip to a VoteResponse");
                    assertEquals(expectedTerms[i], ((VoteResponse) got.get(i)).getTerm(),
                            what + " — frame " + i + " must arrive in send order");
                }
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                fail(what + " — expected terms [" + Arrays.toString(expectedTerms) + "] but had "
                        + got.size() + " frame(s) after 10s");
            }
            awaitMillis(5);
        }
    }

    private static void awaitMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while waiting");
        }
    }
}
