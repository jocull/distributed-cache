package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.cache.ClusterKit;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decision #3 (SCOPE.md) — {@link RaftNode#disconnectFrom(NodeConnectionOutbound)} over a real
 * wire: a 3-node loopback socket cluster (production shape, built by {@link ClusterKit}) where
 * the leader tears down its live link to one follower. The scenario mirrors IT-6/7 of
 * {@code NETWORKING.md} with one twist: the link dies from a {@code disconnectFrom} call, not a
 * partition seal or a crash —
 * <ol>
 *   <li>before: PUT commits 3/3 and applies on every local state machine;</li>
 *   <li>after the disconnect: the surviving pair still commits on a 2-of-3 quorum, the
 *       disconnected follower's view stays at the pre-disconnect value, and the leader's
 *       active-connection list no longer holds a link to the target;</li>
 *   <li>disconnecting the same handle again succeeds as a no-op (idempotence);</li>
 *   <li>the leader re-dials the follower — the follower's own list never lost the (dead) entry
 *       and would skip the re-dial itself — and the follower replays the committed log into
 *       its state machine: three-way convergence on the same value, the leader keeping its
 *       seat the whole time.</li>
 * </ol>
 *
 * <p>Reads go through each node's own {@code DistributedCache} (the local state machine's
 * view), same as {@code RealNetworkClusterTest}; value assertions compare the string content
 * of the {@code value} field, never entry equality.
 */
class RaftNodeDisconnectTest {
    private static final int WAIT_SECONDS = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final String KEY = "disconnect-key";

    @Test
    void testDisconnectFollowerQuorumHoldsAndReconverges() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("no leader before the disconnect", WAIT_SECONDS, UNIT);
            final int target = kit.indexOf(kit.followers().get(0));
            final NodeAddress targetAddress = kit.node(target).getNodeAddress();

            // v1 — before the disconnect, all three are up: the write commits 3/3 and applies
            // on every local state machine.
            final long v1Index = leader
                    .submit(new KeyDataInput(KEY, Map.of("value", "v1".getBytes(StandardCharsets.UTF_8))))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            kit.awaitAppliedAll(v1Index, "the pre-disconnect write did not apply on every node", WAIT_SECONDS, UNIT);

            // Disconnect every link the leader holds to the target follower (in the kit's build
            // ritual that is exactly one socket per pair — but close them all, in either
            // direction).
            final List<NodeConnectionOutbound> links = ((RaftNodeImpl) leader).getActiveConnections().stream()
                    .filter(c -> c.getRemoteNodeAddress().equals(targetAddress))
                    .collect(Collectors.toList());
            assertFalse(links.isEmpty(), "the leader must hold a live link to the target follower");
            for (final NodeConnectionOutbound link : links) {
                leader.disconnectFrom(link).join();
            }
            assertTrue(((RaftNodeImpl) leader).getActiveConnections().stream()
                            .noneMatch(c -> c.getRemoteNodeAddress().equals(targetAddress)),
                    "the leader must no longer hold a link to the target follower");

            // v2 — with one peer gone: the surviving pair commits on a 2-of-3 quorum, the same
            // shape as IT-6/IT-7 (a dead connection dead-marks and drops, the quorum holds).
            final long v2Index = leader
                    .submit(new KeyDataInput(KEY, Map.of("value", "v2".getBytes(StandardCharsets.UTF_8))))
                    .get(WAIT_SECONDS, UNIT).getTermIndex().getIndex();
            assertTrue(v2Index > v1Index, "the two writes must land as two distinct log entries");
            final int leaderSlot = kit.indexOf(leader);
            kit.awaitApplied(leaderSlot, v2Index,
                    "the leader did not apply the committed write", WAIT_SECONDS, UNIT);
            final int otherFollower = theThirdSlot(leaderSlot, target);
            kit.awaitApplied(otherFollower, v2Index,
                    "the surviving follower did not apply the committed write", WAIT_SECONDS, UNIT);

            // The disconnected follower's state machine is frozen at the pre-disconnect view.
            assertEquals("v1", valueOf(kit.localViewOf(target, KEY)),
                    "the target follower's view must stay at the pre-disconnect value");

            // Idempotence: disconnecting the already-gone connection again is a successful no-op.
            leader.disconnectFrom(links.get(0)).join();

            // Re-establish: the leader re-dials the target (its own list lost the address, so
            // connectWithTopology dials it; the target's list still holds the dead entry and
            // would skip a self re-dial). The leader's next heartbeat serves the fresh link.
            leader.connectWithTopology().join();

            // Convergence: the leader pushes the committed log over the fresh link and the
            // target replays it into its state machine.
            kit.awaitApplied(target, v2Index,
                    "the reconnected follower did not converge to the committed log", WAIT_SECONDS, UNIT);
            for (int i = 0; i < kit.nodeCount(); i++) {
                assertEquals("v2", valueOf(kit.localViewOf(i, KEY)),
                        "node " + i + " must converge to the post-disconnect value (same value everywhere)");
                assertEquals(2L, kit.localViewOf(i, KEY).getVersion(),
                        "node " + i + " sees exactly the two entries as versions 1 and 2");
            }

            // The leader never gave its seat up: no quorum could form without it, so no higher
            // term could exist either side of the disconnect.
            assertEquals(1, kit.leaders().size(), "exactly one leader after the reconnect");
            assertTrue(kit.leaders().get(0) == leader, "the leader must keep its seat across the disconnect");
        }
    }

    /** The slot that is neither the leader's nor the target's (the surviving follower). */
    private static int theThirdSlot(int leaderSlot, int target) {
        for (int i = 0; i < 3; i++) {
            if (i != target && i != leaderSlot) {
                return i;
            }
        }
        throw new IllegalStateException("3-node cluster: a third node must exist");
    }

    private static String valueOf(KeyDataOutput view) {
        return new String(view.getData().get("value"), StandardCharsets.UTF_8);
    }
}
