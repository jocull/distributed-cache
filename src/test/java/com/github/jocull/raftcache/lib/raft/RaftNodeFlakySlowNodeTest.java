package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flaky-node / slow-node liveness + safety pins, driven by the per-node fault-injection knob on
 * {@link PassThruMiddleware} ({@code setNodeLatency} / {@code setNodePacketLoss} /
 * {@code clearNodeFaults} / {@code clearAllNodeFaults}, introduced in commit 209696d).
 *
 * <p>Every scenario runs a fixed 3-node cluster (quorum 2), so a single slow or flaky node
 * always leaves a functional quorum of healthy nodes. That is precisely the guarantee under
 * test: the cluster must stay <em>live</em> (elect, commit, make progress) and <em>safe</em>
 * (never two leaders in one term) while one member degrades, and it must recover when the
 * fault is removed.
 *
 * <p>Fault model (see {@code PassThruMiddleware}): a "slow" node adds a latency floor to every
 * hop it touches; a "flaky" node drops each hop it touches with the configured probability
 * (rate 1.0 drops every hop deterministically, reproducing full isolation). The baseline
 * 1-9 ms per-hop delay is always kept, so the fault only adds latency/loss on top.
 *
 * <ul>
 *   <li>{@link #testSlowFollower_clusterStaysLive_andCatchesUpAfterRecovery()} -- the healthy
 *       quorum keeps committing while one member is slow, and the slow node catches up once
 *       its link recovers. (Catch-up <em>while still degraded</em> is <em>not</em> promised by the
 *       basic Raft algorithm -- that is the partition-rejoin disruption documented in Ongaro's
 *       thesis section 9.6, mitigated by the optional Pre-Vote extension etcd enables; see
 *       raft-notes.md.)</li>
 *   <li>{@link #testFlakyFollower_clusterStaysLive()} -- flaky follower (70% loss) never breaks
 *       election/commit of the healthy quorum.</li>
 *   <li>{@link #testSlowLeader_clusterStaysLive_andKeepsCommitting()} -- a leader that turns
 *       slow may or may not be replaced, but the cluster stays live and safe either way.</li>
 *   <li>{@link #testNonLeader_submitFailsFast_notHangs()} -- a non-leader's {@code submit}
 *       fails fast (no hang).</li>
 *   <li>{@link #testKnobContract_boundariesAndDeterminism()} -- the knob's input boundaries and
 *       its deterministic levers.</li>
 * </ul>
 */
class RaftNodeFlakySlowNodeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeFlakySlowNodeTest.class);

    // Latency floor (ms) that makes a node genuinely "slow": every hop it touches is then
    // >= 201 ms, well above the 50 ms heartbeat and the 150-300 ms election timeout, so the
    // node reliably misses heartbeats and lags.
    private static final long SLOW_LATENCY_MILLIS = 200L;
    // A "flaky" node drops 7 of 10 hops per direction (deterministic enough for liveness: the
    // healthy quorum of 2 never depends on it).
    private static final double FLAKY_LOSS_RATE = 0.7;

    // ------------------------------------------------------------------
    // 1. Slow follower: the healthy quorum stays live while one member is slow, and the
    //    slow node catches up once its link recovers.
    // ------------------------------------------------------------------
    @Test
    void testSlowFollower_clusterStaysLive_andCatchesUpAfterRecovery() {
        final InMemoryTopologyDiscovery topo = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware mw = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(topo, mw)) {

            final ClusterNodes nodes = electThreeNodeLeader(topo, mw, raftManager);
            final RaftNode slow = nodes.followers().get(0);

            // Make one follower slow (high-latency). The leader and the other follower stay fast,
            // so they still form the quorum of 2 and can commit without the slow node.
            mw.setNodeLatency(slow.getNodeAddress(), SLOW_LATENCY_MILLIS);

            // LIVENESS DURING THE FAULT -- the contract the basic algorithm actually guarantees
            // here: whichever node holds leadership, the cluster commits a full burst even with
            // one member nearly deaf. Commit through whichever node leads at the moment (never
            // pin to one node: leadership can legitimately move during the fault window). Each
            // submit only returns after it has been committed (majority acknowledged).
            final int batch = 8;
            long lastCommittedIndex = 0;
            for (int i = 0; i < batch; i++) {
                final RaftLog<String> log = commitViaCurrentLeader(mw, "burst-" + i, 10_000L);
                lastCommittedIndex = log.getTermIndex().getIndex();
            }

            // SAFETY DURING THE FAULT: no split brain, and a leader still exists.
            assertAtMostOneLeaderPerTerm(mw);
            assertNotNull(getRaftLeader(mw), "cluster must still have a leader while one member is slow");

            // The slow node is ALLOWED to lag during this window: with a >= 201 ms hop floor its
            // 150-300 ms election timer keeps firing, and the basic algorithm does not promise
            // it replicates while it keeps missing heartbeats. That is exactly the
            // "partitioned server rejoins the cluster" disruption Ongaro's thesis documents in
            // section 9.6 ("Preventing disruptions when a server rejoins the cluster") and
            // mitigates with the optional Pre-Vote extension (etcd has it on by default since
            // v3.5) -- see raft-notes.md. Our contract pins the recovery phase instead:
            //
            // RECONVERGENCE: once the fault is cleared, the cluster must converge -- the slow
            // node catches up to the committed tail it missed.
            mw.clearNodeFaults(slow.getNodeAddress());
            final long requiredIndex = lastCommittedIndex;
            assertWithinTimeout("slow follower " + slow.getId() + " never caught up to index " + requiredIndex
                    + " after the fault cleared (reconvergence)",
                    20, TimeUnit.SECONDS,
                    () -> slow.getCurrentTermIndex().join().getIndex() >= requiredIndex);

            // Final safety: still live, still no split brain.
            assertAtMostOneLeaderPerTerm(mw);
            assertNotNull(getRaftLeader(mw), "cluster must still have a leader after recovery");
        }
    }

    // ------------------------------------------------------------------
    // 2. Flaky follower: intermittent loss must not block the healthy quorum.
    // ------------------------------------------------------------------
    @Test
    void testFlakyFollower_clusterStaysLive() {
        final InMemoryTopologyDiscovery topo = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware mw = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(topo, mw)) {

            final ClusterNodes nodes = electThreeNodeLeader(topo, mw, raftManager);
            final RaftNode flaky = nodes.followers().get(0);

            // Make one follower flaky: each hop it touches is dropped with 70% probability.
            mw.setNodePacketLoss(flaky.getNodeAddress(), FLAKY_LOSS_RATE);

            // The commit quorum for the leader is {leader, other healthy follower} -- it never
            // includes the flaky node -- so the leader must keep committing regardless of how
            // badly the flaky node is losing messages. Commit through whichever node holds
            // leadership when it happens: the flaky node can even win an election (its vote
            // requests land on the good 30%), leaving the original leader as a possibly-lagging
            // follower we must not pin.
            commitViaCurrentLeader(mw, "flaky-present", 15_000L);

            assertAtMostOneLeaderPerTerm(mw);
            assertNotNull(getRaftLeader(mw), "cluster must still have a leader with a flaky follower present");
        }
    }

    // ------------------------------------------------------------------
    // 3. Slow leader: the cluster stays live whether or not it gets replaced.
    // ------------------------------------------------------------------
    @Test
    void testSlowLeader_clusterStaysLive_andKeepsCommitting() {
        final InMemoryTopologyDiscovery topo = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware mw = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(topo, mw)) {

            final ClusterNodes nodes = electThreeNodeLeader(topo, mw, raftManager);
            final RaftNode oldLeader = nodes.leader();
            final long oldLeaderTerm = oldLeader.getCurrentTermIndex().join().getTerm();
            LOGGER.info("===== SLOWING LEADER: {} @ term {} =====", oldLeader.getId(), oldLeaderTerm);

            // Turn the current leader slow: every hop it touches is now >= 201 ms one-way. Two
            // outcomes are both legal and both must leave the cluster live and safe:
            //   (a) the healthy pair's 150-300 ms election timeouts fire before the delayed
            //       heartbeats land, and they elect a leader among themselves. The old leader
            //       may then linger as a stale-term leader (a "zombie") until a higher term
            //       reaches it -- so we deliberately never assert on its state;
            //   (b) the healthy pair simply tolerates the slow heartbeat cadence and the old
            //       leader keeps committing, just slowly.
            // What is NEVER acceptable: a stuck cluster that can no longer commit, or two
            // leaders in one term.
            mw.setNodeLatency(oldLeader.getNodeAddress(), SLOW_LATENCY_MILLIS);

            // Liveness (the real "re-election or slow-commit" proof): whichever node ends up
            // leading, the cluster must commit new entries through the node holding
            // leadership. We do not pin the submit to oldLeader, which may or may not still be
            // the leader when the window closes -- that pin was the flakiness we are removing.
            commitViaCurrentLeader(mw, "post-leader-slowed", 15_000L);

            // Safety + settle: a leader still exists, and the degradation never produced a
            // split brain (two leaders within a single term).
            assertNotNull(getRaftLeader(mw), "cluster must still have a live leader");
            assertAtMostOneLeaderPerTerm(mw);
        }
    }

    // ------------------------------------------------------------------
    // 4. A non-leader's submit must fail fast, not hang (write contract).
    // ------------------------------------------------------------------
    @Test
    void testNonLeader_submitFailsFast_notHangs() {
        final InMemoryTopologyDiscovery topo = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware mw = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(topo, mw)) {

            final ClusterNodes nodes = electThreeNodeLeader(topo, mw, raftManager);

            // Every non-leader (there are exactly two in a 3-node cluster) must reject a submit
            // promptly (completes-with-exception inside 1 s) rather than blocking waiting for a
            // commit that can never happen while it is not the leader. The contract surfaces as an
            // IllegalStateException wrapped in an ExecutionException by CompletableFuture#get.
            for (final RaftNode follower : nodes.followers()) {
                final ExecutionException exception = assertThrows(
                        ExecutionException.class,
                        () -> follower.submit("must-not-commit").get(1, TimeUnit.SECONDS),
                        "non-leader " + follower.getId() + " must reject a submit promptly (fail-fast, not hang)");
                assertEquals(IllegalStateException.class, exception.getCause().getClass(),
                        "the rejection must be the 'not a leader' IllegalStateException, got "
                                + (exception.getCause() == null ? "null" : exception.getCause().getClass()));
            }

            // The leader, by contrast, commits an entry fine (sanity: the fast path works).
            // Commit via the current leader for robustness (leadership may have moved).
            commitViaCurrentLeader(mw, "leader-ok", 5_000L);
            assertAtMostOneLeaderPerTerm(mw);
        }
    }

    // ------------------------------------------------------------------
    // 5. The knob's contract: input boundaries + deterministic behaviour.
    // ------------------------------------------------------------------
    @Test
    void testKnobContract_boundariesAndDeterminism() {
        // -- pure API contract (no cluster): the boundaries the rest of the suite relies on. ----
        try (final PassThruMiddleware mw = new PassThruMiddleware()) {
            final NodeAddress addr = new NodeAddress("knobTarget");

            // Fluent setters all return this, so calls chain.
            assertSame(mw, mw.setNodeLatency(addr, 25L), "setNodeLatency must be fluent");
            assertSame(mw, mw.setNodePacketLoss(addr, 0.5), "setNodePacketLoss must be fluent");
            assertSame(mw, mw.clearNodeFaults(addr), "clearNodeFaults must be fluent");
            assertSame(mw, mw.clearAllNodeFaults(), "clearAllNodeFaults must be fluent");

            // Negative latency is rejected (a "negative floor" is meaningless).
            assertThrows(IllegalArgumentException.class, () -> mw.setNodeLatency(addr, -1L),
                    "negative latency floor must be rejected");

            // Loss rate must be in [0,1]; both sides of the boundary are rejected.
            assertThrows(IllegalArgumentException.class, () -> mw.setNodePacketLoss(addr, -0.0001),
                    "loss rate below 0 must be rejected");
            assertThrows(IllegalArgumentException.class, () -> mw.setNodePacketLoss(addr, 1.0001),
                    "loss rate above 1 must be rejected");

            // Null address is rejected everywhere.
            assertThrows(NullPointerException.class, () -> mw.setNodeLatency(null, 5L),
                    "null address in setNodeLatency must be rejected");
            assertThrows(NullPointerException.class, () -> mw.setNodePacketLoss(null, 0.5),
                    "null address in setNodePacketLoss must be rejected");
            assertThrows(NullPointerException.class, () -> mw.clearNodeFaults(null),
                    "null address in clearNodeFaults must be rejected");
        }

        // -- deterministic behaviour: rate 1.0 IS full isolation; clearNodeFaults IS restore. ----
        // (Mid-range loss is probabilistic and intentionally NOT asserted here; only the two
        //  endpoints are pinned: 1.0 blocks every hop, and clearing reopens the link.)
        final InMemoryTopologyDiscovery topo = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware mw = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(topo, mw)) {

            final ClusterNodes nodes = electThreeNodeLeader(topo, mw, raftManager);
            final RaftNode isolated = nodes.followers().get(0);
            final long isolatedBefore = isolated.getCurrentTermIndex().join().getIndex();

            // 100% loss on one hop side == every message to/from that node is dropped == full
            // isolation. The healthy quorum commits a new entry that the isolated node cannot see.
            mw.setNodePacketLoss(isolated.getNodeAddress(), 1.0);
            final RaftLog<String> commit = commitViaCurrentLeader(mw, "commit-while-isolated", 15_000L);
            final long committedIndex = commit.getTermIndex().getIndex();
            assertTrue(committedIndex > isolatedBefore, "committed index must advance past the pre-fault log");

            // While fully isolated the node cannot replicate the new entry (it cannot receive).
            final long isolatedIndexNow = isolated.getCurrentTermIndex().join().getIndex();
            assertTrue(isolatedIndexNow < committedIndex,
                    "a fully-isolated node (loss 1.0) must lag the freshly committed entry, but was at "
                            + isolatedIndexNow);

            // clearNodeFaults restores the link to baseline: the same node now catches up again.
            mw.clearNodeFaults(isolated.getNodeAddress());
            assertWithinTimeout("isolated node never caught up after clearNodeFaults (link must reopen)",
                    15, TimeUnit.SECONDS,
                    () -> isolated.getCurrentTermIndex().join().getIndex() >= committedIndex);

            assertAtMostOneLeaderPerTerm(mw);
        }
    }

    // ------------------------------------------------------------------
    // Harness (private copies of the minimal helpers the sibling test classes
    // keep inside RaftNodeTest)
    // ------------------------------------------------------------------

    private static ClusterNodes electThreeNodeLeader(InMemoryTopologyDiscovery topo,
                                                     PassThruMiddleware mw,
                                                     RaftManager raftManager) {
        final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
        final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
        final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
        topo.addKnownNode(nodeA.getNodeAddress())
                .addKnownNode(nodeB.getNodeAddress())
                .addKnownNode(nodeC.getNodeAddress());
        mw.addNode(nodeA).addNode(nodeB).addNode(nodeC);

        // Starting the nodes kicks off the election timeouts.
        mw.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
        mw.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

        return assertResultWithinTimeout("Did not get a 3-node cluster with a leader and two followers",
                5, TimeUnit.SECONDS, () -> {
            assertAtMostOneLeaderPerTerm(mw);
            final ClusterNodes result = getRaftLeaderAndFollowers(mw);
            if (result.leader() != null && result.followers().size() == 2) {
                return result;
            }
            return null;
        });
    }

    private static ClusterNodes getRaftLeaderAndFollowers(PassThruMiddleware middleware) {
        final RaftNode leader = getRaftLeader(middleware);
        final List<RaftNode> leaders = getRaftLeaders(middleware);
        final List<RaftNode> followers = getRaftFollowers(middleware);
        final List<RaftNode> candidates = getRaftCandidates(middleware);
        return new ClusterNodes() {
            @Override
            public RaftNode leader() {
                return leader;
            }

            @Override
            public List<RaftNode> leaders() {
                return leaders;
            }

            @Override
            public List<RaftNode> candidates() {
                return candidates;
            }

            @Override
            public List<RaftNode> followers() {
                return followers;
            }
        };
    }

    private static List<RaftNode> getRaftFollowers(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getNodeState().join().equals(NodeStates.FOLLOWER))
                .collect(Collectors.toList());
    }

    private static List<RaftNode> getRaftCandidates(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getNodeState().join().equals(NodeStates.CANDIDATE))
                .collect(Collectors.toList());
    }

    private static RaftNode getRaftLeader(PassThruMiddleware middleware) {
        return middleware.getAddressRaftNodeMap().values().stream()
                .filter(r -> r.getNodeState().join().equals(NodeStates.LEADER))
                .max(Comparator.comparingInt(r -> r.getCurrentTermIndex().join().getTerm()))
                .orElse(null);
    }

    private static List<RaftNode> getRaftLeaders(PassThruMiddleware middleware) {
        final List<RaftNode> leaders = new ArrayList<>();
        for (final RaftNode node : middleware.getAddressRaftNodeMap().values()) {
            if (node.getNodeState().join().equals(NodeStates.LEADER)) {
                leaders.add(node);
            }
        }
        return leaders;
    }

    // Raft safety (election-safety): at most one node may be LEADER within any single term.
    // Two leaders at the same time is only legal when their terms differ. A violation (two
    // leaders at one term) is never legal, even transiently, so hard-throw on the sample that
    // sees it.
    private static void assertAtMostOneLeaderPerTerm(PassThruMiddleware middleware) {
        final Map<Integer, List<String>> leadersByTerm = new TreeMap<>();
        for (final RaftNode node : middleware.getAddressRaftNodeMap().values()) {
            if (node.getNodeState().join().equals(NodeStates.LEADER)) {
                leadersByTerm.computeIfAbsent(node.getCurrentTermIndex().join().getTerm(), t -> new ArrayList<>())
                        .add(node.getId());
            }
        }
        leadersByTerm.forEach((term, nodeIds) -> {
            if (nodeIds.size() > 1) {
                throw new AssertionError("Split brain: " + nodeIds.size() + " nodes are LEADER at term " + term
                        + " (" + nodeIds + ") -- at most one leader is allowed per term");
            }
        });
    }

    // Proves "the cluster can commit" without pinning the submit to one specific node: polls
    // whichever node currently claims leadership (highest term), submits the entry, and retries
    // within the deadline if that node was demoted in the meantime (IllegalStateException) or
    // could not commit this window (TimeoutException). This is the only safe way to pin liveness
    // in scenarios where leadership may legitimately move (slow leader, flaky node winning an
    // election), including the case where a stale-term leader lingers as a zombie.
    private static RaftLog<String> commitViaCurrentLeader(PassThruMiddleware mw, String entry, long timeoutMillis) {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        Throwable lastCause = null;
        while (System.currentTimeMillis() < deadline) {
            final RaftNode currentLeader = getRaftLeader(mw);
            if (currentLeader != null) {
                try {
                    return currentLeader.submit(entry).get(3, TimeUnit.SECONDS);
                } catch (final ExecutionException ex) {
                    // Demoted (or a term race) between the poll and the commit: retry.
                    lastCause = ex.getCause();
                } catch (final TimeoutException ex) {
                    // Holds leadership but this window could not commit (flaky link):
                    // retry and hope a later window is better, or leadership moves.
                    lastCause = ex;
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("test interrupted while waiting for commit of " + entry, ex);
                }
            }
            sleepQuietly(50L);
        }
        throw new AssertionError("cluster never committed " + entry + " within "
                + timeoutMillis + " ms of submitting through the current leader; last failure: " + lastCause);
    }

    private static void sleepQuietly(long millis) {
        try {
            //noinspection BusyWait
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("test interrupted", ex);
        }
    }

    private interface ClusterNodes {
        RaftNode leader();

        List<RaftNode> leaders();

        List<RaftNode> candidates();

        List<RaftNode> followers();
    }

    private static void assertWithinTimeout(String message, long wait, TimeUnit unit, Supplier<Boolean> fnAction) {
        assertResultWithinTimeout(message, wait, unit, () -> (fnAction.get() ? Boolean.TRUE : null));
    }

    private static <T> T assertResultWithinTimeout(String message, long wait, TimeUnit unit, Supplier<T> fnAction) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(wait);
        while (System.currentTimeMillis() < deadline) {
            final T result = fnAction.get();
            if (result != null) {
                return result;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError(message, ex);
            }
        }
        throw new AssertionError(message);
    }
}
