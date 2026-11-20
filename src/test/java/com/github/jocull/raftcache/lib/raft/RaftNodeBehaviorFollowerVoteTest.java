package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
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
        public void sendAppendEntries(AppendEntries appendEntries) {
        }

        @Override
        public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        }

        List<VoteResponse> voteResponses() {
            return List.copyOf(voteResponses);
        }
    }
}
