package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the pre-vote contract (etcd-style "pre-vote") for every node role.
 *
 * <p>Pre-vote is availability, not correctness: a behind or quarantined node must not burn
 * real terms it would never win. The guarantees pinned here:
 *
 * <ul>
 *   <li><b>Vote gate (voters)</b> — a pre-vote request is a <em>hypothetical</em> vote: it is
 *       granted iff the node would grant a real vote for that same term (term rule plus the
 *       Raft §5.4.2 up-to-date rule), and it must change <b>no</b> state — no term advance,
 *       no {@code votedFor} burn, no timer reset, no role change.</li>
 *   <li><b>One-vote-per-term survives</b> — because pre-vote never sets {@code votedFor}, a real
 *       same-term vote arriving afterwards is still correctly granted (and still denied after
 *       a real grant has burned the vote).</li>
 *   <li><b>Collector (follower)</b> — the election timeout starts a round for {@code term + 1};
 *       a denial or a foreign-term response is not counted; a majority of real grants (self plus
 *       grants) converts to candidate; below majority the node stays a follower; a live leader
 *       (AppendEntries) invalidates an in-flight round so late grants cannot resurrect it.</li>
 *   <li><b>Roles that never collect</b> — a leader's or candidate's response handler ignores
 *       every pre-vote response; pre-vote responses never advance a term.</li>
 * </ul>
 */
class RaftNodeBehaviorPreVoteTest {
    /** The voter's own log ends at (term 1, index 5). */
    private static final int OWN_TERM = 1;
    private static final long OWN_LAST_INDEX = 5L;

    /** Convenience for the messages-package TermIndex (clashes with the package-level one). */
    private static com.github.jocull.raftcache.lib.raft.messages.TermIndex ti(int term, long index) {
        return new com.github.jocull.raftcache.lib.raft.messages.TermIndex(term, index);
    }

    private static void appendLog(RaftNodeImpl node, long count) {
        for (long i = 1; i <= count; i++) {
            node.logs.appendLog(OWN_TERM, "entry-" + i);
        }
    }

    private static void registerPeer(RaftNodeImpl node, String peerId) {
        node.clusterTopology.register(new NodeIdentifier(peerId, new NodeAddress(peerId + "-addr")));
    }

    private static int termOf(RaftNodeImpl node) {
        return ((RaftNodeBehavior) node.currentReceiver()).term;
    }

    private static NodeStates stateOf(RaftNodeImpl node) {
        return ((RaftNodeBehavior) node.currentReceiver()).state;
    }

    // ---- Voter side: the follower as a pre-vote VOTER ----

    @Test
    void follower_voter_grantsWhenTermAcceptedAndLogAtLeastAsUpToDate() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voter-addr"));
            middleware.addNode(voter);
            appendLog(voter, OWN_LAST_INDEX);
            voter.start().join(); // started FOLLOWER of term 0

            // Boundary: candidate last log EQUAL to ours (1,5) -> up-to-date -> grant.
            final RecordingSender equalCandidate = new RecordingSender("equalCandidate");
            voter.currentReceiver().onPreVoteRequest(equalCandidate, new PreVoteRequest(1, "equalCandidate", ti(OWN_TERM, OWN_LAST_INDEX)));
            assertTrue(equalCandidate.preVoteResponses().size() == 1, "equal-index candidate must get a response");
            assertTrue(equalCandidate.preVoteResponses().get(0).isPreVoteGranted(), "equal-index candidate is up-to-date (Raft §5.4.2) -> grant");

            // Strictly fresher candidate (1,6) -> grant.
            final RecordingSender fresherCandidate = new RecordingSender("fresherCandidate");
            voter.currentReceiver().onPreVoteRequest(fresherCandidate, new PreVoteRequest(1, "fresherCandidate", ti(OWN_TERM, OWN_LAST_INDEX + 1)));
            assertTrue(fresherCandidate.preVoteResponses().size() == 1, "fresher candidate must get a response");
            assertTrue(fresherCandidate.preVoteResponses().get(0).isPreVoteGranted(), "fresher log -> grant");

            // A pre-vote request must not have changed the voter: same role, same term.
            assertEquals(NodeStates.FOLLOWER, stateOf(voter), "pre-vote must not change the role");
            assertEquals(0, termOf(voter), "pre-vote must not advance the term");
        }
    }

    @Test
    void follower_voter_deniesWhenLogIsStale_butStillResponds() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voter-addr"));
            middleware.addNode(voter);
            appendLog(voter, OWN_LAST_INDEX);
            voter.start().join();

            // (1,3) < own (1,5) -> the up-to-date rule must deny.
            final RecordingSender staleCandidate = new RecordingSender("staleCandidate");
            voter.currentReceiver().onPreVoteRequest(staleCandidate, new PreVoteRequest(1, "staleCandidate", ti(OWN_TERM, 3L)));
            assertTrue(staleCandidate.preVoteResponses().size() == 1, "a denial must still be responded to");
            assertFalse(staleCandidate.preVoteResponses().get(0).isPreVoteGranted(), "stale log -> deny");

            assertEquals(NodeStates.FOLLOWER, stateOf(voter), "pre-vote must not change the role");
            assertEquals(0, termOf(voter), "pre-vote must not advance the term");
        }
    }

    @Test
    void follower_voter_deniesWhenTermIsLower_evenWithFresherLog() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voter-addr"));
            middleware.addNode(voter);
            appendLog(voter, OWN_LAST_INDEX);
            voter.start().join();

            // Bring the voter up to term 1 the way a real election does: a vote request for
            // term 1 converts the term-0 follower to a term-1 follower (and grants a real
            // vote, burning votedFor — irrelevant here but pinned in the next test).
            final RecordingSender seeder = new RecordingSender("seeder");
            voter.currentReceiver().onVoteRequest(seeder, new VoteRequest(1, "seeder", ti(OWN_TERM, OWN_LAST_INDEX)));
            seeder.assertOneVoteGranted();
            assertEquals(NodeStates.FOLLOWER, stateOf(voter));
            assertEquals(1, termOf(voter), "the vote request must have converted the follower to term 1");

            // A pre-vote for a LOWER term is refused even though its log (9,9) is far fresher:
            // the term gate (follower: req.term >= term) is evaluated before up-to-date.
            final RecordingSender lowerTerm = new RecordingSender("lowerTerm");
            voter.currentReceiver().onPreVoteRequest(lowerTerm, new PreVoteRequest(0, "lowerTerm", ti(9, 9L)));
            assertTrue(lowerTerm.preVoteResponses().size() == 1, "a lower-term pre-vote must still get a response");
            assertFalse(lowerTerm.preVoteResponses().get(0).isPreVoteGranted(), "lower term -> deny regardless of log");

            assertEquals(1, termOf(voter), "pre-vote must not change the term");
        }
    }

    @Test
    void preVoteMustNotBurnTheRealVote_andTheRealVoteStillBurnsOnGrant() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voter-addr"));
            middleware.addNode(voter);
            appendLog(voter, OWN_LAST_INDEX);
            voter.start().join();

            // Establish term 1 via a real vote (stale candidate -> denied, voter is now a
            // term-1 follower with votedFor still null — denials must not burn the vote).
            final RecordingSender staleSeeder = new RecordingSender("staleSeeder");
            voter.currentReceiver().onVoteRequest(staleSeeder, new VoteRequest(1, "staleSeeder", ti(OWN_TERM, 3L)));
            assertFalse(stateOf(voter) != NodeStates.FOLLOWER || termOf(voter) != 1, "voter must now be a term-1 follower");
            staleSeeder.assertOneVoteDenied();

            // Two pre-votes handled by that SAME term-1 follower instance: a denial and a grant.
            final RecordingSender pvStale = new RecordingSender("pvStale");
            voter.currentReceiver().onPreVoteRequest(pvStale, new PreVoteRequest(1, "pvStale", ti(OWN_TERM, 3L)));
            pvStale.assertOnePreVoteDenied();
            final RecordingSender pvFresh = new RecordingSender("pvFresh");
            voter.currentReceiver().onPreVoteRequest(pvFresh, new PreVoteRequest(1, "pvFresh", ti(OWN_TERM, OWN_LAST_INDEX)));
            pvFresh.assertOnePreVoteGranted();

            // THE DISCRIMINATOR: the pre-votes must NOT have consumed the one-vote-per-term —
            // a real same-term candidate arriving now must still be granted. (If the handler
            // had mutated votedFor, the guard would have silenced this response entirely.)
            final RecordingSender realCandidate = new RecordingSender("realCandidate");
            voter.currentReceiver().onVoteRequest(realCandidate, new VoteRequest(1, "realCandidate", ti(OWN_TERM, OWN_LAST_INDEX)));
            realCandidate.assertOneVoteGranted();

            // Sanity: the REAL grant did burn the vote — a further same-term candidate is refused.
            final RecordingSender laterCandidate = new RecordingSender("laterCandidate");
            voter.currentReceiver().onVoteRequest(laterCandidate, new VoteRequest(1, "laterCandidate", ti(OWN_TERM, OWN_LAST_INDEX)));
            assertTrue(laterCandidate.voteResponses().isEmpty(), "one vote per term: a second same-term candidate gets no grant");
        }
    }

    // ---- Voter side: leader and candidate as pre-vote VOTERS ----

    @Test
    void candidate_voter_deniesSameTerm_grantsStrictlyHigher_neverChangesState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            appendLog(node, OWN_LAST_INDEX);
            // Two peers -> majority 2, so the candidate below will NOT self-elect.
            registerPeer(node, "peerA");
            registerPeer(node, "peerB");
            node.start().join();
            node.convertToCandidate(1); // deterministic CANDIDATE of term 1
            assertEquals(NodeStates.CANDIDATE, stateOf(node));
            assertEquals(1, termOf(node));

            final RecordingSender inquirer = new RecordingSender("inquirer");
            // Same term: a candidate's real vote path refuses same-term requests -> deny.
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(1, "other", ti(OWN_TERM, OWN_LAST_INDEX)));
            // Strictly higher term + up-to-date log -> grant.
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(2, "other", ti(OWN_TERM, OWN_LAST_INDEX)));
            // Strictly higher term but stale log -> deny (up-to-date rule).
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(2, "other", ti(OWN_TERM, 3L)));

            assertEquals(3, inquirer.preVoteResponses().size(), "every pre-vote request must get a response");
            assertFalse(inquirer.preVoteResponses().get(0).isPreVoteGranted(), "same-term pre-vote to a candidate -> deny");
            assertTrue(inquirer.preVoteResponses().get(1).isPreVoteGranted(), "higher-term + up-to-date pre-vote -> grant");
            assertFalse(inquirer.preVoteResponses().get(2).isPreVoteGranted(), "higher-term but stale-log pre-vote -> deny");

            // A pre-vote RESPONSE to a candidate is fire-and-forget: never advances term / role.
            node.currentReceiver().onPreVoteResponse(inquirer, new PreVoteResponse(99, true));
            assertEquals(NodeStates.CANDIDATE, stateOf(node), "pre-vote response must not change the role");
            assertEquals(1, termOf(node), "pre-vote response must not advance the term");
        }
    }

    @Test
    void leader_voter_deniesSameTerm_grantsStrictlyHigher_neverChangesState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            appendLog(node, OWN_LAST_INDEX);
            // Two peers -> majority 2, so the candidate does NOT self-elect and we can
            // deterministically step it up to leader.
            registerPeer(node, "peerA");
            registerPeer(node, "peerB");
            node.start().join();
            node.convertToCandidate(1);
            node.convertToLeader(); // deterministic LEADER of term 1
            assertEquals(NodeStates.LEADER, stateOf(node));
            assertEquals(1, termOf(node));

            final RecordingSender inquirer = new RecordingSender("inquirer");
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(1, "other", ti(OWN_TERM, OWN_LAST_INDEX)));
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(2, "other", ti(OWN_TERM, OWN_LAST_INDEX)));
            node.currentReceiver().onPreVoteRequest(inquirer, new PreVoteRequest(2, "other", ti(OWN_TERM, 3L)));

            assertEquals(3, inquirer.preVoteResponses().size(), "every pre-vote request must get a response");
            assertFalse(inquirer.preVoteResponses().get(0).isPreVoteGranted(), "same-term pre-vote to a leader -> deny");
            assertTrue(inquirer.preVoteResponses().get(1).isPreVoteGranted(), "higher-term + up-to-date pre-vote -> grant");
            assertFalse(inquirer.preVoteResponses().get(2).isPreVoteGranted(), "higher-term but stale-log pre-vote -> deny");

            // Pre-vote responses must never demote / advance a leader.
            node.currentReceiver().onPreVoteResponse(inquirer, new PreVoteResponse(99, true));
            assertEquals(NodeStates.LEADER, stateOf(node), "pre-vote response must not demote the leader");
            assertEquals(1, termOf(node), "pre-vote response must not advance the term");
        }
    }

    // ---- Collector side: the follower as a pre-vote COLLECTOR ----

    @Test
    void follower_collector_singleNode_shortcutsDirectlyToCandidate() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            // One node -> majority 1: own pre-vote is already a majority; no peer will
            // ever respond, so the round must shortcut to a real election.
            node.start().join();
            assertEquals(NodeStates.FOLLOWER, stateOf(node));
            assertEquals(0, termOf(node));

            ((RaftNodeBehaviorFollower) node.currentReceiver()).onElectionTimeout();

            // The conversion to candidate is synchronous; the candidate's own self-election
            // to leader is dispatched asynchronously, so the node may already be the LEADER
            // of term 1 here. Both are exactly "converted to a real election for term 1".
            assertNotEquals(NodeStates.FOLLOWER, stateOf(node), "own pre-vote majority must shortcut to a real election");
            assertEquals(1, termOf(node), "the shortcut must advance the term exactly once (0 -> 1)");
        }
    }

    @Test
    void follower_collector_majorityOfGrants_convertsToCandidate() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            registerPeer(node, "peerA");
            registerPeer(node, "peerB"); // 3 nodes -> majority 2: self + 1 grant
            node.start().join();
            appendLog(node, OWN_LAST_INDEX); // deterministic last log the round reports

            ((RaftNodeBehaviorFollower) node.currentReceiver()).onElectionTimeout(); // round for term 1
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a round below majority must not convert");
            assertEquals(0, termOf(node), "a round below majority must not advance the term");

            // ONE grant in a 3-node cluster reaches the majority (self + 1).
            final RecordingSender peerA = new RecordingSender("peerA");
            node.currentReceiver().onPreVoteResponse(peerA, new PreVoteResponse(1, true));
            assertEquals(NodeStates.CANDIDATE, stateOf(node), "a majority of pre-vote grants must convert to candidate");
            assertEquals(1, termOf(node), "the conversion must advance the term exactly once (0 -> 1)");
        }
    }

    @Test
    void follower_collector_denialsAndForeignTerms_neverCount_staysFollower() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            registerPeer(node, "peerA");
            registerPeer(node, "peerB"); // 3 nodes -> majority 2
            node.start().join();

            ((RaftNodeBehaviorFollower) node.currentReceiver()).onElectionTimeout(); // round for term 1

            // A DENIAL must not count — counting one would let a behind node reach a
            // majority out of refusions.
            final RecordingSender peerA = new RecordingSender("peerA");
            node.currentReceiver().onPreVoteResponse(peerA, new PreVoteResponse(1, false));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a denial must change nothing");
            assertEquals(0, termOf(node), "a denial must not advance the term");

            // FOREIGN-term grants (both directions) belong to someone else's round — never counted.
            final RecordingSender peerB = new RecordingSender("peerB");
            node.currentReceiver().onPreVoteResponse(peerB, new PreVoteResponse(2, true));
            node.currentReceiver().onPreVoteResponse(peerB, new PreVoteResponse(0, true));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "foreign-term responses must not convert");
            assertEquals(0, termOf(node), "foreign-term responses must not advance the term");
        }
    }

    @Test
    void follower_collector_appendEntriesInvalidateTheRound_lateGrantsIgnored() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
            middleware.addNode(node);
            registerPeer(node, "peerA");
            registerPeer(node, "peerB"); // 3 nodes -> majority 2
            node.start().join();

            ((RaftNodeBehaviorFollower) node.currentReceiver()).onElectionTimeout(); // round for term 1, in flight
            assertEquals(NodeStates.FOLLOWER, stateOf(node));

            // A live leader heartbeats at our term: the in-flight pre-vote round is moot.
            final RecordingSender leader = new RecordingSender("leader");
            node.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, ti(0, 0L), ti(0, 0L), Collections.emptyList()));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a same-term heartbeat must not convert the follower");
            assertEquals(0, termOf(node), "a same-term heartbeat must not advance the term");

            // THE DISCRIMINATOR: a late grant for the dead round must be ignored — if the
            // round had not been invalidated, this single grant would (self + 1) reach the
            // majority of a 3-node cluster and silently convert the node to candidate.
            final RecordingSender peerA = new RecordingSender("peerA");
            node.currentReceiver().onPreVoteResponse(peerA, new PreVoteResponse(1, true));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a grant after the round was invalidated must not convert");
            assertEquals(0, termOf(node), "a grant after the round was invalidated must not advance the term");
        }
    }

    /**
     * A minimal {@link NodeConnectionOutbound} that identifies itself as a given peer and
     * records the {@link VoteResponse}s and {@link PreVoteResponse}s it is asked to send.
     * Everything else is inert.
     */
    private static final class RecordingSender implements NodeConnectionOutbound {
        private final String remoteNodeId;
        private final List<VoteResponse> voteResponses = new ArrayList<>();
        private final List<PreVoteResponse> preVoteResponses = new ArrayList<>();

        RecordingSender(String remoteNodeId) {
            this.remoteNodeId = remoteNodeId;
        }

        @Override
        public String getRemoteNodeId() {
            return remoteNodeId;
        }

        @Override
        public void setRemoteNodeId(String remoteNodeId) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public NodeAddress getRemoteNodeAddress() {
            return new NodeAddress("stub-" + remoteNodeId);
        }

        @Override
        public TermIndex getTermIndex() {
            return new TermIndex(0, 0L);
        }

        @Override
        public void setTermIndex(TermIndex termIndex) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public Instant getLastEntriesSent() {
            return Instant.EPOCH;
        }

        @Override
        public void setLastEntriesSent(Instant lastEntriesSent) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public Instant getLastEntriesAcknowledged() {
            return Instant.EPOCH;
        }

        @Override
        public void setLastEntriesAcknowledged(Instant lastEntriesAcknowledged) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendIntroduction(Introduction introduction) {
        }

        @Override
        public void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        }

        @Override
        public CompletableFuture<StateResponse> sendStateRequest(StateRequest stateRequest) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendStateResponse(StateResponse stateResponse) {
        }

        @Override
        public void sendVoteRequest(VoteRequest voteRequest) {
        }

        @Override
        public void sendVoteResponse(VoteResponse voteResponse) {
            voteResponses.add(voteResponse);
        }

        @Override
        public void sendPreVoteRequest(PreVoteRequest preVoteRequest) {
        }

        @Override
        public void sendPreVoteResponse(PreVoteResponse preVoteResponse) {
            preVoteResponses.add(preVoteResponse);
        }

        @Override
        public void sendAppendEntries(AppendEntries appendEntries) {
        }

        @Override
        public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        }

        List<VoteResponse> voteResponses() {
            return List.copyOf(voteResponses);
        }

        List<PreVoteResponse> preVoteResponses() {
            return List.copyOf(preVoteResponses);
        }

        void assertOneVoteGranted() {
            assertTrue(voteResponses.size() == 1, "expected exactly one vote response");
            assertTrue(voteResponses.get(0).isVoteGranted(), "expected the vote to be granted");
        }

        void assertOneVoteDenied() {
            assertTrue(voteResponses.size() == 1, "expected exactly one vote response (a denial, not silence)");
            assertFalse(voteResponses.get(0).isVoteGranted(), "expected the vote to be denied");
        }

        void assertOnePreVoteGranted() {
            assertTrue(preVoteResponses.size() == 1, "expected exactly one pre-vote response");
            assertTrue(preVoteResponses.get(0).isPreVoteGranted(), "expected the pre-vote to be granted");
        }

        void assertOnePreVoteDenied() {
            assertTrue(preVoteResponses.size() == 1, "expected exactly one pre-vote response (a denial, not silence)");
            assertFalse(preVoteResponses.get(0).isPreVoteGranted(), "expected the pre-vote to be denied");
        }
    }
}
