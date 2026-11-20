package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.data.StateMachineBridge;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class RaftNodeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeTest.class);

    private static Stream<Arguments> provideIterationsLong() {
        return IntStream.range(1, 31)
                .mapToObj(Arguments::of);
    }

    private static Stream<Arguments> provideIterationsShort() {
        return IntStream.range(1, 6)
                .mapToObj(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testNodeIntroductions(int iteration) {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            middleware.addNode(nodeA).addNode(nodeB);

            assertEquals(Set.of("nodeA"), nodeA.getKnownNodes().join().stream()
                    .map(NodeIdentifier::getId)
                    .collect(Collectors.toSet()));
            assertEquals(Set.of("nodeB"), nodeB.getKnownNodes().join().stream()
                    .map(NodeIdentifier::getId)
                    .collect(Collectors.toSet()));

            nodeA.connectTo(nodeB.getNodeAddress());

            // nodeA self registers, and contacts nodeB
            assertWithinTimeout("nodeA's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeA.getKnownNodes().join().stream()
                        .map(NodeIdentifier::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
            assertWithinTimeout("nodeB's topology did not settle with nodeA, nodeB", 1000, TimeUnit.MILLISECONDS, () -> {
                final Set<String> nodeTopology = nodeB.getKnownNodes().join().stream()
                        .map(NodeIdentifier::getId)
                        .collect(Collectors.toSet());
                return Set.of("nodeA", "nodeB").equals(nodeTopology);
            });
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElection(int iteration) throws Throwable {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // No node should be a leader right now
            assertFalse(NodeStates.LEADER.equals(nodeA.getNodeState().join())
                    || NodeStates.LEADER.equals(nodeB.getNodeState().join())
                    || NodeStates.LEADER.equals(nodeC.getNodeState().join()), "No leader should have been found!");

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // Some super hacks to deal with eventual consistency of in-memory leader/followers vs query state
            final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();
            try {
                assertWithinTimeout("Queried cluster state did not align with node states", 5, TimeUnit.SECONDS, () -> {
                    final ClusterNodes nodes = getRaftLeaderAndFollowers(middleware);
                    final List<StateResponse> clusterNodeStates = nodeA.getClusterNodeStates().join();
                    try {
                        assertNotNull(clusterNodeStates);
                        assertEquals(3, clusterNodeStates.size());
                        assertEquals(Set.of("nodeA", "nodeB", "nodeC"), clusterNodeStates.stream().map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));
                        assertEquals(Stream.of("addressA", "addressB", "addressC").map(NodeAddress::new).collect(Collectors.toSet()),
                                clusterNodeStates.stream().map(x -> x.getIdentifier().getNodeAddress()).collect(Collectors.toSet()));
                        assertEquals(Set.of("nodeA", "nodeB", "nodeC"), clusterNodeStates.stream().map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));
                        assertEquals(nodes.leader().getId(), clusterNodeStates.stream().filter(c -> c.getState().equals(NodeStates.LEADER)).findFirst().orElseThrow().getIdentifier().getId());
                        assertEquals(nodes.followers().stream().map(RaftNode::getId).collect(Collectors.toSet()),
                                clusterNodeStates.stream().filter(c -> c.getState().equals(NodeStates.FOLLOWER)).map(x -> x.getIdentifier().getId()).collect(Collectors.toSet()));

                        final StateResponse leaderState = nodeA.getLeader().join().orElseThrow();
                        assertEquals(nodes.leader().getId(), leaderState.getIdentifier().getId());
                        assertEquals(nodes.leader().getCurrentTermIndex().join().getTerm(), leaderState.getTerm());
                    } catch (Throwable error) {
                        lastThrowable.set(error);
                        return false;
                    }
                    return true;
                });
            } catch (Throwable error) {
                if (lastThrowable.get() == null) {
                    throw error;
                }
                throw lastThrowable.get();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElectionWithFailure_oneNode(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // The old leader must not survive the partition as a stale leader. After the network is
            // restored it has heard of the higher term and converted to follower -- or won a fresh
            // election at a higher term, in which case it again IS the current leader. Staying
            // LEADER at an overtaken term while another node holds leadership is the bug this
            // guards against; the previous test could not see it at all.
            final RaftNode oldLeader = nodes1.leader();
            assertWithinTimeout("Old leader " + oldLeader.getId() + " never stepped down after network restore -- it must be a follower, or have won a fresh election at a higher term",
                    5, TimeUnit.SECONDS,
                    () -> oldLeader.getNodeState().join() != NodeStates.LEADER || getRaftLeader(middleware) == oldLeader);
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsLong")
    void testElectionWithFailure_twoNodes(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {}, {} =====", nodes1.leader().getId(), nodes1.followers().get(0).getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            middleware.interrupt(nodes1.followers().get(0).getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and candidates", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.candidates().size() == 2) {
                    return result;
                }
                return null;
            });

            LOGGER.info("===== RESTORING NETWORK: {}, {} =====", nodes1.leader().getId(), nodes1.followers().get(0).getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            middleware.restore(nodes1.followers().get(0).getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // The old leader must not survive the partition as a stale leader (see the matching
            // assertion in testElectionWithFailure_oneNode).
            final RaftNode oldLeader = nodes1.leader();
            assertWithinTimeout("Old leader " + oldLeader.getId() + " never stepped down after network restore -- it must be a follower, or have won a fresh election at a higher term",
                    5, TimeUnit.SECONDS,
                    () -> oldLeader.getNodeState().join() != NodeStates.LEADER || getRaftLeader(middleware) == oldLeader);
        }
    }

    // Regression pin for the election "up-to-date" rule (Raft §5.4.1 / §5.4.2, Decision
    // #1 in SCOPE.md): a voter grants its vote only if the candidate's LAST log entry is
    // at least as recent as the voter's own -- entry term first, then index
    // (RaftNodeBehaviorFollower.onVoteRequest against RaftLogs.getCurrentTermIndex).
    //
    // PassThruMiddleware.interrupt(node) severs every channel touching that node, so a
    // node is either fully inside the live partition or an isolated singleton: it cannot
    // lag while staying reachable to the other voters. The reachable form of that
    // scenario -- the window this rule exists to police -- is a stale node REJOINING the
    // live quorum before it has caught up:
    //
    //   1. 3 nodes (quorum 2): elect a leader L; commit a baseline entry to all three.
    //   2. isolate a follower S; L commits entry E with the live pair {L, fresh F}.
    //      S now strictly lags F (and L) on every measure: last AND committed.
    //   3. isolate L too: it remains a (stale) LEADER of its own one-node partition at
    //      the old term, which the per-term leadership assertion tolerates.
    //   4. restore S into the live quorum {S, F} WHILE IT STILL LAGS.
    //
    // From there the outcome is deterministic, whichever of S or F times out first:
    //   - S's VoteRequests fail the last-entry comparison at F (S strictly lags), so S
    //     can never collect the 2-vote quorum -- it has only its own vote, ever;
    //   - F outranks S on log recency, so F always collects {F, S} and wins, at a term
    //     strictly higher than L's.
    //
    // OLD VS NEW RULE -- honest framing. The fix (commit 6b33a9c) changed the
    // candidate's VoteRequest to carry the log's LAST entry (Raft §5.4.1) instead of
    // its committed entry. committed ⊆ last (a commit can never lead the log), so the
    // old committed-based rule is strictly MORE conservative: it can deny a vote the
    // new last-entry rule grants (a liveness deviation in the old code), and never the
    // reverse (a safety violation). At vote time here every voter has committed ==
    // last, so BOTH rules elect the same winner: this test is a PIN of the rule's
    // observable behavior, not a discriminator between the two rules. The
    // uncommitted-tail consequence (a longer, lower-term tail must be rolled back once
    // the higher-term leader wins) is pinned by testUpToDateRuleUncommittedTailLoses
    // below.
    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testUpToDateRuleStaleCandidateLoses(int iteration) throws InterruptedException {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            final RaftNode oldLeader = nodes1.leader();
            final RaftNode staleFollower = nodes1.followers().get(0);
            final RaftNode freshFollower = nodes1.followers().get(1);

            // Baseline: commit an entry while the cluster is whole, so the stale and
            // fresh followers hold the SAME last entry when the split forms and the
            // later divergence is purely the partition's doing. This makes the
            // up-to-date comparison at vote time a tie on last-entry TERM resolved by
            // the index (Raft §5.4.2 tie-break), rather than a trivial 0-vs-1 term
            // mismatch that an empty-log node would produce.
            assertDoesNotThrow(
                    () -> oldLeader.submit("up-to-date-rule-baseline").get(5, TimeUnit.SECONDS),
                    "Expected the leader to commit the baseline entry to all three nodes");

            // Isolate one follower so it misses the next committed entry -- it becomes the
            // stale candidate of the partition.
            LOGGER.info("===== INTERRUPTING NETWORK: {} (stale follower) =====", staleFollower.getId());
            middleware.interrupt(staleFollower.getNodeAddress());

            // Commit an entry quorum of 2 (oldLeader + freshFollower). After this, the stale
            // follower strictly lags BOTH on current AND committed log -- the up-to-date
            // comparison is a pure log-position test from here on.
            final RaftLog<String> committedEntry = assertDoesNotThrow(
                    () -> oldLeader.submit("up-to-date-rule-pin").get(5, TimeUnit.SECONDS),
                    "Expected the leader to commit while the stale follower was isolated");

            // Now isolate the leader too. It remains a (stale) LEADER of its own one-node
            // partition at its old term; the per-term leadership assertion tolerates that
            // zombie because it lives at the old term, and while isolated it cannot
            // affect any vote.
            LOGGER.info("===== INTERRUPTING NETWORK: {} (old leader) =====", oldLeader.getId());
            middleware.interrupt(oldLeader.getNodeAddress());

            // Bring the stale follower back into the live partition WHILE IT STILL LAGS:
            // the live partition is now {stale, fresh} -- exactly the 2-of-3 quorum --
            // and the stale node has not caught up. interrupt() severs EVERY channel
            // touching a node, so a lagging voter cannot stay reachable to its other
            // voters: this reachable form, a stale node rejoining ahead of its catch-up,
            // IS the window the up-to-date rule exists to police.
            LOGGER.info("===== RESTORING {} (stale follower, still lagging) -- election window on the {stale, fresh} partition =====", staleFollower.getId());
            middleware.restore(staleFollower.getNodeAddress());

            // Deterministic outcome for the {stale, fresh} partition, whichever of the two
            // times out first:
            //  - stale CANNOT win: every VoteRequest it sends fails the up-to-date
            //    comparison at the fresh voter (stale strictly lags on the last entry:
            //    same entry term, LOWER index -- the §5.4.2 tie-break), and it holds only
            //    its own vote, never the 2-vote quorum;
            //  - fresh ALWAYS wins: its self-vote plus the stale voter's (granted on log
            //    recency) IS the quorum, and a term race between them can only ever
            //    resolve in favour of the fresh candidate, since stale can never
            //    outvote it.
            // The split must therefore settle as freshFollower LEADER at a term strictly
            // higher than the old leader's, with the stale follower never in LEADER
            // state at any sample.
            final int oldLeaderTerm = oldLeader.getCurrentTermIndex().join().getTerm();
            assertResultWithinTimeout("Fresh follower " + freshFollower.getId() + " did not win the split election", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                if (!NodeStates.LEADER.equals(freshFollower.getNodeState().join())) {
                    return null;
                }
                final RaftNode leader = getRaftLeader(middleware);
                if (leader == null || leader.getId().equals(oldLeader.getId())) {
                    return null;
                }
                if (freshFollower.getCurrentTermIndex().join().getTerm() <= oldLeaderTerm) {
                    return null;
                }
                if (NodeStates.LEADER.equals(staleFollower.getNodeState().join())) {
                    throw new AssertionError("Stale follower " + staleFollower.getId() + " won the split election -- it lags the fresh follower on the up-to-date rule and must never win");
                }
                return freshFollower;
            });

            // Let the split run a few election windows: the stale follower must never
            // appear as a leader, and never may two leaders coexist within one term.
            LOGGER.info("===== HOLDING SPLIT: verifying stale follower never becomes leader =====");
            assertWithinTimeout("Stale follower " + staleFollower.getId() + " never confirmed as non-leader across the split window", 2, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                return !NodeStates.LEADER.equals(staleFollower.getNodeState().join())
                        && NodeStates.LEADER.equals(freshFollower.getNodeState().join());
            });

            // Restore ONLY the zombie leader (the stale follower is already in the live
            // partition, following the new leader). The old leader carries the SAME log
            // as the new leader -- it had replicated the entry it committed before the
            // partition -- so no truncation is needed: it must simply hear the higher
            // term, step down to follower, and agree on the committed position.
            LOGGER.info("===== RESTORING NETWORK: {} (zombie leader) =====", oldLeader.getId());
            middleware.restore(oldLeader.getNodeAddress());

            // Re-converged cluster: exactly one leader plus two followers, and every node
            // agrees on the CURRENT and COMMITTED log position. (The leader may be
            // freshFollower or a node that won a fresh election after the restore -- what
            // must hold is a single leader and identical logs everywhere.)
            final ClusterNodes settled = assertResultWithinTimeout("Cluster did not converge to a single leader with identical committed state after network restore", 10, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() == null || result.followers().size() != 2) {
                    return null;
                }
                if (!middleware.getAddressRaftNodeMap().values().stream()
                        .allMatch(r -> r.getCurrentTermIndex().join().equals(result.leader().getCurrentTermIndex().join()))) {
                    return null;
                }
                if (!middleware.getAddressRaftNodeMap().values().stream()
                        .allMatch(r -> r.getCommittedTermIndex().join().equals(result.leader().getCommittedTermIndex().join()))) {
                    return null;
                }
                // The committed entry must be part of every node's committed log
                if (committedEntry.getTermIndex().compareTo(result.leader().getCommittedTermIndex().join()) > 0) {
                    return null;
                }
                return result;
            });
            final TermIndex unifiedCurrent = settled.leader().getCurrentTermIndex().join();
            final TermIndex unifiedCommitted = settled.leader().getCommittedTermIndex().join();

            // The old leader must not survive as a stale leader: it heard of the higher term
            // and converted to follower -- or won a fresh election at a higher term.
            assertWithinTimeout("Old leader " + oldLeader.getId() + " never stepped down after network restore -- it must be a follower, or have won a fresh election at a higher term",
                    5, TimeUnit.SECONDS,
                    () -> oldLeader.getNodeState().join() != NodeStates.LEADER || getRaftLeader(middleware) == oldLeader);

            // The stale follower must have caught up: its current log includes the entry
            // it was isolated from, and its committed position matches the cluster's
            assertEquals(unifiedCurrent, staleFollower.getCurrentTermIndex().join(),
                    "Stale follower did not catch up after restore");
            assertEquals(unifiedCommitted, staleFollower.getCommittedTermIndex().join(),
                    "Stale follower's committed position did not converge after restore");
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testLogReplication(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes.followers().forEach(r -> {
                final ExecutionException exception = assertThrows(ExecutionException.class, () -> r.submit("hello").get(1, TimeUnit.SECONDS));
                assertEquals(IllegalStateException.class, exception.getCause().getClass());
            });

            final RaftLog<String> log1 = assertDoesNotThrow(
                    () -> nodes.leader().submit("hello").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 1");
            assertTrue(nodes.followers().stream().allMatch(r -> log1.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Followers didn't get log 1");

            final RaftLog<String> log2 = assertDoesNotThrow(
                    () -> nodes.leader().submit("hello again").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 2");
            assertTrue(nodes.followers().stream().allMatch(r -> log2.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Followers didn't get log 2");

            final RaftLog<String> log3 = assertDoesNotThrow(
                    () -> nodes.leader().submit("hello one last time").get(1, TimeUnit.SECONDS),
                    "Expected followers to get log 2");
            assertTrue(nodes.followers().stream().allMatch(r -> log3.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Followers didn't get log 3");

            final List<CompletableFuture<RaftLog<Integer>>> futures = IntStream.range(0, 1000)
                    .mapToObj(i -> nodes.leader().submit(i))
                    .collect(Collectors.toList());

            final List<RaftLog<Integer>> results = futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            final RaftLog<Integer> logLast = results.get(futures.size() - 1);
            assertTrue(nodes.followers().stream().allMatch(r -> logLast.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Followers didn't get final log");
        }
    }

    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testStateMachineApply(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware);
             // In production each node process has its own manager, bus, map, and bridge.
             // This in-process harness shares one bus across all nodes, so one bridge
             // (and one map) covers the whole cluster -- which is also what proves the
             // bridge's idempotency, since every node emits the same committed entries.
             final StateMachineBridge bridge = new StateMachineBridge(raftManager.getEventBus(), partitionMap)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // Non-data log entries must not disturb the state machine, but must not
            // block later entries either
            assertDoesNotThrow(
                    () -> nodes.leader().submit("a plain, non-data entry").get(1, TimeUnit.SECONDS),
                    "Expected leader to commit the non-data entry");

            final KeyDataInput firstWrite = new KeyDataInput("state-machine-key", Map.of("value", "first write".getBytes(StandardCharsets.UTF_8)));
            assertDoesNotThrow(
                    () -> nodes.leader().submit(firstWrite).get(1, TimeUnit.SECONDS),
                    "Expected leader to commit the first KeyDataInput");

            final KeyDataInput secondWrite = new KeyDataInput("state-machine-key", Map.of("value", "second write".getBytes(StandardCharsets.UTF_8)));
            final RaftLog<KeyDataInput> logSecondWrite = assertDoesNotThrow(
                    () -> nodes.leader().submit(secondWrite).get(1, TimeUnit.SECONDS),
                    "Expected leader to commit the second KeyDataInput");

            // The leader only acknowledges the client after the state machine has synchronously
            // applied the entry (commit -> publish LogsCommitted -> notifyCommitted), so by the
            // time the last submit's future returned, the bridge has already applied every entry
            // up to the last committed index. This assert is deliberately immediate -- it is the
            // test of the apply-then-acknowledge guarantee, and needing a timeout poll here would
            // mean the ordering regressed
            assertTrue(bridge.getLastAppliedIndex() >= logSecondWrite.getTermIndex().getIndex(),
                    "Bridge applied index " + bridge.getLastAppliedIndex() + " is below the last committed index "
                            + logSecondWrite.getTermIndex().getIndex()
                            + " -- commit must only be acknowledged after it has been applied");

            // The bridge has passed through every committed entry (including the non-data one),
            // so the latest committed write must be the one visible in the state machine
            final KeyDataOutput output = partitionMap.getData("state-machine-key")
                    .orElseThrow(() -> new AssertionError("Expected state-machine-key to be present after the bridge caught up"));
            assertEquals("state-machine-key", output.getKey());
            assertEquals("second write", new String(output.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be the latest committed write");
            assertEquals(2L, output.getVersion(), "Both committed writes should be applied exactly once (no duplicate delivery on the shared bus)");

            // Followers should have received the committed entries (their own commit emission
            // re-applied against the shared map without side effects)
            assertTrue(nodes.followers().stream().allMatch(r -> logSecondWrite.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Followers didn't receive the committed entries");
        }
    }

    // A one-node cluster is its own quorum: it must be able to elect itself, commit its
    // own entries, and apply them. Before the fixes, none of this worked:
    // - the candidate only converted to leader on an external vote response, so the
    //   single node looped candidate -> candidate forever
    // - the leader's updateCommittedIndex only counted follower acknowledgements, so
    //   with nothing to acknowledge the commit future never completed
    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testSingleNodeElectionCommitApply(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware);
             final StateMachineBridge bridge = new StateMachineBridge(raftManager.getEventBus(), partitionMap)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress());
            middleware.addNode(nodeA);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final RaftNode leader = assertResultWithinTimeout("Did not get expected single-node leader", 5, TimeUnit.SECONDS, () -> {
                final RaftNode result = getRaftLeader(middleware);
                if (result != null && "nodeA".equals(result.getId())) {
                    return result;
                }
                return null;
            });

            final KeyDataInput firstWrite = new KeyDataInput("single-node-key", Map.of("value", "lone".getBytes(StandardCharsets.UTF_8)));
            final RaftLog<KeyDataInput> logFirstWrite = assertDoesNotThrow(
                    () -> leader.submit(firstWrite).get(5, TimeUnit.SECONDS),
                    "Expected the single node to commit its own entry (its own log is the quorum)");

            assertTrue(bridge.getLastAppliedIndex() >= logFirstWrite.getTermIndex().getIndex(),
                    "Bridge applied index " + bridge.getLastAppliedIndex()
                            + " is below the committed index " + logFirstWrite.getTermIndex().getIndex()
                            + " -- commit must only be acknowledged after it has been applied");

            final KeyDataOutput output = partitionMap.getData("single-node-key")
                    .orElseThrow(() -> new AssertionError("Expected single-node-key to be present after the bridge caught up"));
            assertEquals("single-node-key", output.getKey());
            assertEquals("lone", new String(output.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be the committed write");
            assertEquals(1L, output.getVersion(), "Entry should be applied exactly once");
        }
    }

    // A two-node cluster needs 2 votes / 2 replicated copies to commit (majority of 2).
    // Before the quorum fix, the leader's updateCommittedIndex only counted follower
    // acknowledgements (1 replica < majority 2), so no entry could ever commit -- the
    // submit future would hang. This test hangs forever (well, to the join timeout)
    // without the leader-counting-quorum fix.
    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testTwoNodeCommitApply(int iteration) {
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware);
             final StateMachineBridge bridge = new StateMachineBridge(raftManager.getEventBus(), partitionMap)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes = assertResultWithinTimeout("Did not get expected leader and follower", 5, TimeUnit.SECONDS, () -> {
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            final KeyDataInput firstWrite = new KeyDataInput("two-node-key", Map.of("value", "paired".getBytes(StandardCharsets.UTF_8)));
            final RaftLog<KeyDataInput> logFirstWrite = assertDoesNotThrow(
                    () -> nodes.leader().submit(firstWrite).get(5, TimeUnit.SECONDS),
                    "Expected the two-node cluster to commit (leader + follower = quorum of 2)");

            assertTrue(nodes.followers().stream().allMatch(r -> logFirstWrite.getTermIndex().compareTo(r.getCurrentTermIndex().join()) >= 0),
                    "Follower didn't receive the committed entry");

            assertTrue(bridge.getLastAppliedIndex() >= logFirstWrite.getTermIndex().getIndex(),
                    "Bridge applied index " + bridge.getLastAppliedIndex()
                            + " is below the committed index " + logFirstWrite.getTermIndex().getIndex()
                            + " -- commit must only be acknowledged after it has been applied");

            final KeyDataOutput output = partitionMap.getData("two-node-key")
                    .orElseThrow(() -> new AssertionError("Expected two-node-key to be present after the bridge caught up"));
            assertEquals("two-node-key", output.getKey());
            assertEquals("paired", new String(output.getData().get("value"), StandardCharsets.UTF_8), "Applied entry should be the committed write");
            assertEquals(1L, output.getVersion(), "Entry should be applied exactly once");
        }
    }

    @SuppressWarnings("BusyWait")
    @ParameterizedTest
    @MethodSource("provideIterationsShort")
    void testLogReplicationDuringTermChange(int iteration) throws InterruptedException {
        final AtomicBoolean producerStopped = new AtomicBoolean();
        final InMemoryTopologyDiscovery inMemoryTopologyDiscovery = new InMemoryTopologyDiscovery();
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(inMemoryTopologyDiscovery, middleware)) {
            final RaftNode nodeA = raftManager.newNode("nodeA", new NodeAddress("addressA"));
            final RaftNode nodeB = raftManager.newNode("nodeB", new NodeAddress("addressB"));
            final RaftNode nodeC = raftManager.newNode("nodeC", new NodeAddress("addressC"));
            inMemoryTopologyDiscovery
                    .addKnownNode(nodeA.getNodeAddress())
                    .addKnownNode(nodeB.getNodeAddress())
                    .addKnownNode(nodeC.getNodeAddress());
            middleware.addNode(nodeA).addNode(nodeB).addNode(nodeC);

            // Starting the nodes will begin election timeouts
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.connectWithTopology().join());
            middleware.getAddressRaftNodeMap().values().forEach(raftNode -> raftNode.start().join());

            final ClusterNodes nodes1 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            nodes1.followers().forEach(r -> {
                final ExecutionException exception = assertThrows(ExecutionException.class, () -> r.submit("hello").get(1, TimeUnit.SECONDS));
                assertEquals(IllegalStateException.class, exception.getCause().getClass());
            });

            final List<CompletableFuture<RaftLog<Integer>>> logFutures = new ArrayList<>();
            final Set<RaftNode> seenLeaderNodes = new HashSet<>();
            final Function<Integer, CompletableFuture<RaftLog<Integer>>> fnSubmitLogFuture = (count) -> {
                final RaftNode currentLeader = getRaftLeader(middleware);
                if (currentLeader == null) {
                    LOGGER.warn("No current leader!");
                    throw new RuntimeException("No current leader @ " + count);
                }
                synchronized (seenLeaderNodes) {
                    seenLeaderNodes.add(currentLeader);
                }

                try {
                    final CompletableFuture<RaftLog<Integer>> future = currentLeader.submit(count);
                    LOGGER.debug("Added {} to current leader {}", count, currentLeader.getId());
                    return future;
                } catch (Exception ex) {
                    LOGGER.debug("Failed to add {} to current leader {}", count, currentLeader.getId(), ex);
                    throw new RuntimeException("Rejected @ " + count, ex);
                }
            };

            final AtomicInteger produceCounter = new AtomicInteger();
            final AtomicInteger retryCounter = new AtomicInteger();
            final Thread producer = new Thread(() -> {
                try {
                    final Retry retry = Retry.of("producer-retry", RetryConfig.custom()
                            .maxAttempts(10)
                            .waitDuration(Duration.ofSeconds(1))
                            .retryOnException(e -> {
                                LOGGER.error("retrying for exception", e);
                                retryCounter.incrementAndGet();
                                return true;
                            })
                            .build());

                    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
                    while (!producerStopped.get()) {
                        final int thisCount = produceCounter.incrementAndGet();
                        final CompletableFuture<RaftLog<Integer>> result = retry.executeCompletionStage(
                                        executor,
                                        () -> fnSubmitLogFuture.apply(thisCount))
                                .toCompletableFuture();

                        logFutures.add(result);
                        Thread.sleep(5);
                    }
                } catch (InterruptedException ex) {
                    LOGGER.info("Interrupted - exiting", ex);
                }
            });
            producer.setName("producer-thread");
            producer.setDaemon(true);
            producer.start();

            final int logStartPoint = assertResultWithinTimeout("Starting logs did not populate", 5, TimeUnit.SECONDS, () -> {
                synchronized (logFutures) {
                    if (logFutures.size() >= 100) {
                        return logFutures.size();
                    }
                    return null;
                }
            });

            LOGGER.info("===== INTERRUPTING NETWORK: {} =====", nodes1.leader().getId());
            middleware.interrupt(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes2 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 1) {
                    return result;
                }
                return null;
            });

            // Stay interrupted for a while, expect that we collect more logs and see more nodes
            assertWithinTimeout("Did not see new leader during interrupt", 5, TimeUnit.SECONDS, () -> {
                boolean leadersPassed;
                synchronized (seenLeaderNodes) {
                    leadersPassed = seenLeaderNodes.size() >= 2;
                }
                return leadersPassed;
            });

            final int logStartPoint2 = assertResultWithinTimeout("Starting logs did not populate", 5, TimeUnit.SECONDS, () -> {
                synchronized (logFutures) {
                    if (logFutures.size() >= 100) {
                        return logFutures.size();
                    }
                    return null;
                }
            });
            assertWithinTimeout("Did not get more logs with new leader during interrupt", 5, TimeUnit.SECONDS, () -> {
                boolean logsPassed;
                synchronized (logFutures) {
                    logsPassed = logFutures.size() >= (logStartPoint2 + 200);
                }
                return logsPassed;
            });

            LOGGER.info("===== RESTORING NETWORK: {} =====", nodes1.leader().getId());
            middleware.restore(nodes1.leader().getNodeAddress());
            final ClusterNodes nodes3 = assertResultWithinTimeout("Did not get expected leaders and followers", 5, TimeUnit.SECONDS, () -> {
                assertAtMostOneLeaderPerTerm(middleware);
                final ClusterNodes result = getRaftLeaderAndFollowers(middleware);
                if (result.leader() != null && result.followers().size() == 2) {
                    return result;
                }
                return null;
            });

            // The old leader must not survive the partition as a stale leader (see the matching
            // assertion in testElectionWithFailure_oneNode).
            final RaftNode oldLeader = nodes1.leader();
            assertWithinTimeout("Old leader " + oldLeader.getId() + " never stepped down after network restore -- it must be a follower, or have won a fresh election at a higher term",
                    5, TimeUnit.SECONDS,
                    () -> oldLeader.getNodeState().join() != NodeStates.LEADER || getRaftLeader(middleware) == oldLeader);

            // Transactions sent to the previous leader are unable to hit a majority and will stall until the
            // leader comes back online. When it does, it will rollback and cancel pending requests to it.
            LOGGER.info("===== WAITING FOR TRANSACTIONS TO HIT RETRIES =====");
            assertWithinTimeout("Did not see transactions previously sent to old leader revert and retry", 5, TimeUnit.SECONDS,
                    () -> retryCounter.get() > 1);

            LOGGER.info("===== WAITING FOR PRODUCER TO STOP =====");
            producerStopped.set(true);
            assertWithinTimeout("Producer didn't stop!", 1, TimeUnit.SECONDS, () -> !producer.isAlive());

            LOGGER.info("===== WAITING FOR LAST LOG TO COMPLETE =====");
            final RaftLog<Integer> raftLogLast = assertDoesNotThrow(() -> logFutures.get(logFutures.size() - 1).get(30, TimeUnit.SECONDS));
            assertNotNull(raftLogLast);
            assertEquals(produceCounter.get(), raftLogLast.getEntry());
        } finally {
            producerStopped.set(true);
        }
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

    // Raft safety (the election-safety guarantee): at most one node may be in LEADER state
    // within any single term. Two nodes in LEADER state at the same time is only legal when
    // their terms differ -- e.g. the old leader, partitioned off, that has not yet heard of
    // the higher term. The violation below (two leaders at one term) is never legal, even
    // transiently. It is a hard throw so it fails the test on the very sample that sees it,
    // instead of merely producing one more "unacceptable" poll iteration that expires quietly
    // into the 5-second timeout.
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

    private interface ClusterNodes {
        RaftNode leader();

        List<RaftNode> leaders();

        List<RaftNode> candidates();

        List<RaftNode> followers();
    }

    private void assertWithinTimeout(String message, long wait, TimeUnit unit, Supplier<Boolean> fnAction) {
        assertResultWithinTimeout(message, wait, unit, () -> {
            if (fnAction.get()) {
                return true;
            }
            return null;
        });
    }

    private <T> T assertResultWithinTimeout(String message, long wait, TimeUnit unit, Supplier<T> fnAction) {
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
                throw new RuntimeException(ex);
            }
        }
        throw new AssertionError(message);
    }
}
