package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.CacheDeleteRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheExpireRequest;
import com.github.jocull.raftcache.lib.raft.messages.CachePutRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadResponse;
import com.github.jocull.raftcache.lib.raft.messages.CacheWriteResponse;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the voter-side grant/deny contract for {@link RaftNodeBehaviorFollower#onVoteRequest}.
 *
 * <p>Per Raft §5.4.1 a node grants at most one vote per term, but a <em>denial</em> must NOT
 * "burn" that single vote: it has to leave {@code votedForNodeId} null so that a more up-to-date
 * candidate, arriving later in the same term, can still be granted.
 *
 * <p>The historical bug was that a denial set {@code votedForNodeId} to the voter's own id
 * ("vote for self instead"), which closed the guard at {@code onVoteRequest} for the rest of the
 * term. A voter that saw a stale candidate first then also refused the freshest candidate in the
 * same term, costing an entire empty term of liveness. This test pins the fixed behaviour and
 * will fail against the old code (the fresher candidate receives no {@code VoteResponse}).
 */
class RaftNodeBehaviorFollowerVoteTest {
    private static final int TERM = 1;
    private static final int VOTER_LAST_INDEX = 5; // voter's log is (term 1, index 5)
    private static final int STALE_CANDIDATE_INDEX = 3;  // (1,3) <  (1,5) -> must be denied
    private static final int FRESH_CANDIDATE_INDEX = 5;  // (1,5) >= (1,5) -> must be granted

    @Test
    void denialMustNotBurnTheVote_soFresherCandidateInSameTerm_IsStillGranted() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voterAddress"));
            middleware.addNode(voter);

            // Give the voter an up-to-date log: five entries in term 1 so its current index is (1, 5).
            for (int i = 1; i <= VOTER_LAST_INDEX; i++) {
                voter.logs.appendLog(TERM, "entry-" + i);
            }
            assertEquals(5L, voter.logs.getCurrentTermIndex().getIndex(), "sanity: voter log index");
            assertEquals(TERM, voter.logs.getCurrentTermIndex().getTerm(), "sanity: voter log term");

            // Start the voter so it is a started FOLLOWER; the first vote request below advances it to term 1.
            voter.start().join();

            // Two candidates, both in term 1: a stale one (older log) and a fresher one (as up-to-date).
            final RecordingSender staleCandidate = new RecordingSender("staleCandidate");
            final RecordingSender freshCandidate = new RecordingSender("freshCandidate");

            // 1) Stale candidate arrives first in the term -> up-to-date rule must deny it.
            voter.currentReceiver().onVoteRequest(staleCandidate,
                    new VoteRequest(TERM, "staleCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, STALE_CANDIDATE_INDEX)));
            // 2) The fresher candidate arrives later in the SAME term -> must still be granted.
            voter.currentReceiver().onVoteRequest(freshCandidate,
                    new VoteRequest(TERM, "freshCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, FRESH_CANDIDATE_INDEX)));

            // The stale candidate must have received exactly one response, and it must be a DENY.
            assertEquals(1, staleCandidate.voteResponses().size(), "stale candidate must get exactly one vote response");
            assertFalse(staleCandidate.voteResponses().get(0).isVoteGranted(),
                    "an older-log candidate must be refused by the up-to-date rule");

            // THE DISCRIMINATOR: the fresher candidate must ALSO get one response, and it must be a GRANT.
            // (Under the old "vote for self instead" denial behaviour the guard closes and the fresher
            // candidate receives NO response here -- so this assertion fails against the old code.)
            assertEquals(1, freshCandidate.voteResponses().size(),
                    "a denial must not burn the voter's single vote: the fresher candidate must still receive a response");
            assertTrue(freshCandidate.voteResponses().get(0).isVoteGranted(),
                    "a more up-to-date candidate arriving later in the same term must be granted");
        }
    }

    // ---- Stage 6(i): a hostile wire tail on the vote path is refused before any mutation ----

    /**
     * Stage 6(i): the vote-request tail (1, -1) is constructible in the bare wire carrier, but its
     * negative axis would IAE out of the domain build in {@code onVoteRequest} (the stage 6(g)
     * root mechanism, this time on the vote path) — and that build happens AFTER the accept path
     * has already cleared the leader. The refusal must precede all of that: exactly one explicit
     * DENIAL carrying the voter's own committed tail, and (discriminator at the end) the voter's
     * single per-term vote must NOT be burned by the denial.
     */
    @Test
    void hostileTail_refusedBeforeMutation_negativeIndexTail() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voterAddress"));
            middleware.addNode(voter);

            for (int i = 1; i <= VOTER_LAST_INDEX; i++) {
                voter.logs.appendLog(TERM, "entry-" + i);
            }
            voter.start().join(); // started FOLLOWER of term 0

            // The denial's committed tail must be the voter's OWN tail, read BEFORE the request
            // (robust to whatever commit state appendLog leaves the log in).
            final TermIndex committedBefore = voter.logs.getCommittedTermIndex();

            final RecordingSender hostile = new RecordingSender("hostileCandidate");
            voter.currentReceiver().onVoteRequest(hostile,
                    new VoteRequest(TERM, "hostileCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, -1L)));

            // Exactly one explicit denial — not an IAE out of the domain build, not silence.
            assertEquals(1, hostile.voteResponses().size(), "a hostile tail must be refused with an explicit response, not silence");
            assertFalse(hostile.voteResponses().get(0).isVoteGranted(), "a hostile tail must never be granted");
            assertEquals(TERM, hostile.voteResponses().get(0).getTerm(), "the denial must echo the accepted term");
            assertEquals(committedBefore.getTerm(), hostile.voteResponses().get(0).getCommittedTermIndex().getTerm(),
                    "the denial must carry the voter's own committed tail");
            assertEquals(committedBefore.getIndex(), hostile.voteResponses().get(0).getCommittedTermIndex().getIndex(),
                    "the denial must carry the voter's own committed tail");

            // The hostile request carried a higher term: term recognition still happens FIRST —
            // the 0 -> 1 conversion precedes the refusal — and nothing else about the voter changed.
            assertEquals(NodeStates.FOLLOWER, ((RaftNodeBehavior) voter.currentReceiver()).state, "the refusal must not change the role");
            assertEquals(TERM, ((RaftNodeBehavior) voter.currentReceiver()).term, "a higher-term request must still advance the term (conversion before refusal)");

            // THE DISCRIMINATOR: the refusal must NOT have burned votedForNodeId — a legitimate
            // up-to-date candidate in the SAME term must still be granted. (If the refusal had
            // set votedForNodeId, the same-term guard at the top of onVoteRequest would silence
            // this response entirely, so this assertion fails against a vote-burning refusal.)
            final RecordingSender legitimate = new RecordingSender("legitimateCandidate");
            voter.currentReceiver().onVoteRequest(legitimate,
                    new VoteRequest(TERM, "legitimateCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, VOTER_LAST_INDEX)));
            assertEquals(1, legitimate.voteResponses().size(),
                    "a denial must not burn the vote: the legitimate same-term candidate must still receive a response");
            assertTrue(legitimate.voteResponses().get(0).isVoteGranted(), "a legitimate up-to-date same-term candidate must be granted");
        }
    }

    /**
     * Stage 6(i): the null-tail variant of the hostile vote path — the wire field is missing
     * entirely, which would NPE out of the wire getters before the domain constructor's IAE
     * could even fire. Same contract as the negative-axis pin: one explicit DENIAL carrying the
     * voter's own committed tail, term conversion intact, and the single per-term vote unburned.
     */
    @Test
    void hostileTail_refusedBeforeMutation_nullTail() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voterAddress"));
            middleware.addNode(voter);

            for (int i = 1; i <= VOTER_LAST_INDEX; i++) {
                voter.logs.appendLog(TERM, "entry-" + i);
            }
            voter.start().join(); // started FOLLOWER of term 0

            final TermIndex committedBefore = voter.logs.getCommittedTermIndex();

            final RecordingSender hostile = new RecordingSender("nullTailCandidate");
            voter.currentReceiver().onVoteRequest(hostile,
                    new VoteRequest(TERM, "nullTailCandidate", null));

            // Exactly one explicit denial — not an NPE out of the wire getters, not silence.
            assertEquals(1, hostile.voteResponses().size(), "a null tail must be refused with an explicit response, not silence");
            assertFalse(hostile.voteResponses().get(0).isVoteGranted(), "a null tail must never be granted");
            assertEquals(TERM, hostile.voteResponses().get(0).getTerm(), "the denial must echo the accepted term");
            assertEquals(committedBefore.getTerm(), hostile.voteResponses().get(0).getCommittedTermIndex().getTerm(),
                    "the denial must carry the voter's own committed tail");
            assertEquals(committedBefore.getIndex(), hostile.voteResponses().get(0).getCommittedTermIndex().getIndex(),
                    "the denial must carry the voter's own committed tail");

            assertEquals(NodeStates.FOLLOWER, ((RaftNodeBehavior) voter.currentReceiver()).state, "the refusal must not change the role");
            assertEquals(TERM, ((RaftNodeBehavior) voter.currentReceiver()).term, "a higher-term request must still advance the term (conversion before refusal)");

            final RecordingSender legitimate = new RecordingSender("legitimateCandidate");
            voter.currentReceiver().onVoteRequest(legitimate,
                    new VoteRequest(TERM, "legitimateCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, VOTER_LAST_INDEX)));
            assertEquals(1, legitimate.voteResponses().size(),
                    "a denial must not burn the vote: the legitimate same-term candidate must still receive a response");
            assertTrue(legitimate.voteResponses().get(0).isVoteGranted(), "a legitimate up-to-date same-term candidate must be granted");
        }
    }

    // ---- Stage 6(l): an unbound sender identity is refused before any mutation ----

    /**
     * Stage 6(l): a vote request from a connection whose remoteNodeId was never bound is
     * malformed by construction (onIntroduction is the only binder, and the transport layer
     * throws on an unbound vote carrier before it can arrive at the handler). Granting it
     * anyway would record {@code votedForNodeId = null}, which the same-term guard
     * ({@code votedForNodeId != null}) silently bypasses — handing out a SECOND grant in the
     * same term, violating Raft §5.2's one-vote-per-term. The refusal must precede even the
     * higher-term conversion (so the null sender does not get free term recognition), and it
     * must not burn the voter's single vote: the discriminator below proves both.
     */
    @Test
    void unboundSender_onVoteRequest_refusedBeforeConversion_voteNotBurned() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voterAddress"));
            middleware.addNode(voter);
            for (int i = 1; i <= VOTER_LAST_INDEX; i++) {
                voter.logs.appendLog(TERM, "entry-" + i);
            }
            voter.start().join(); // started FOLLOWER of term 0

            final RecordingSender unbound = new RecordingSender(null);
            voter.currentReceiver().onVoteRequest(unbound,
                    new VoteRequest(TERM, "unboundCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, VOTER_LAST_INDEX)));

            // Identity-family shape: NO reply, and ZERO state change — not even the
            // 0 -> 1 term conversion that the higher-term request would otherwise trigger.
            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.FOLLOWER, ((RaftNodeBehavior) voter.currentReceiver()).state, "the refusal must not change the role");
            assertEquals(0, ((RaftNodeBehavior) voter.currentReceiver()).term, "the refusal must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the voter's single true vote must be UNBURNED — a legitimate
            // bound candidate in the same term still gets its one, and only one, grant.
            final RecordingSender legitimate = new RecordingSender("legitimateCandidate");
            voter.currentReceiver().onVoteRequest(legitimate,
                    new VoteRequest(TERM, "legitimateCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, VOTER_LAST_INDEX)));
            assertEquals(1, legitimate.voteResponses().size(), "a legitimate candidate must still receive its one vote response");
            assertTrue(legitimate.voteResponses().get(0).isVoteGranted(), "a legitimate up-to-date same-term candidate must be granted");
            assertEquals(TERM, ((RaftNodeBehavior) voter.currentReceiver()).term, "the legitimate request takes the term conversion");

            // Sanity: one vote per term — a further same-term candidate receives nothing.
            final RecordingSender later = new RecordingSender("laterCandidate");
            voter.currentReceiver().onVoteRequest(later,
                    new VoteRequest(TERM, "laterCandidate", new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, VOTER_LAST_INDEX)));
            assertTrue(later.voteResponses().isEmpty(), "one vote per term: a second same-term candidate gets no grant");
            ((RaftNodeBehavior) voter.currentReceiver()).close();
        }
    }

    /**
     * Stage 6(l): a vote RESPONSE from an unbound sender carries an unattributable voter. The
     * follower's only reaction to this surface is the higher-term branch (convertToFollower),
     * so the refusal must fire before it: the null sender must not earn even a step-down.
     * (The transport layer already throws on this on the live path — this pins the
     * direct-handler surface, the family's whole reason for existence.)
     */
    @Test
    void unboundSender_onVoteResponse_doesNotAdvanceTerm() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl voter = (RaftNodeImpl) raftManager.newNode("voter", new NodeAddress("voterAddress"));
            middleware.addNode(voter);
            voter.start().join(); // started FOLLOWER of term 0

            final RecordingSender unbound = new RecordingSender(null);
            voter.currentReceiver().onVoteResponse(unbound,
                    new VoteResponse(TERM, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, 0L), false));

            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.FOLLOWER, ((RaftNodeBehavior) voter.currentReceiver()).state, "a null-sender response must not change the role");
            assertEquals(0, ((RaftNodeBehavior) voter.currentReceiver()).term, "a null-sender response must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the step-down path is not burned — a BOUND higher-term
            // response still converts the follower up to that term.
            final RecordingSender bound = new RecordingSender("boundVoter");
            voter.currentReceiver().onVoteResponse(bound,
                    new VoteResponse(TERM, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(TERM, 0L), false));
            assertEquals(NodeStates.FOLLOWER, ((RaftNodeBehavior) voter.currentReceiver()).state, "a bound higher-term response still steps down to follower");
            assertEquals(TERM, ((RaftNodeBehavior) voter.currentReceiver()).term, "a bound higher-term response still advances the term");
            ((RaftNodeBehavior) voter.currentReceiver()).close();
        }
    }

    /**
     * A minimal {@link NodeConnectionOutbound} that identifies itself as a given candidate and
     * records the {@link VoteResponse}s it is asked to send. Everything else is inert.
     */
    private static final class RecordingSender implements NodeConnectionOutbound {
        private final String remoteNodeId;
        private final List<VoteResponse> voteResponses = new ArrayList<>();

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
        }

        @Override
        public void sendAppendEntries(AppendEntries appendEntries) {
        }

        @Override
        public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        }

        @Override
        public void sendInstallSnapshot(InstallSnapshot installSnapshot) {
        }

        @Override
        public void sendInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse) {
        }

        // Client B first cut (B §3.3, 2026-11-25): the cache verb seams are not exercised by these behavior tests.
        @Override
        public CompletableFuture<CacheWriteResponse> sendCachePut(CachePutRequest request) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public CompletableFuture<CacheWriteResponse> sendCacheDelete(CacheDeleteRequest request) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public CompletableFuture<CacheWriteResponse> sendCacheExpire(CacheExpireRequest request) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public CompletableFuture<CacheReadResponse> sendCacheRead(CacheReadRequest request) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendCacheWriteResponse(CacheWriteResponse response) {
        }

        @Override
        public void sendCacheReadResponse(CacheReadResponse response) {
        }

        List<VoteResponse> voteResponses() {
            return List.copyOf(voteResponses);
        }
    }
}
