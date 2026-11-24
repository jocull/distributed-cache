package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
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
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 6(l) (SNAPSHOTS.md stage 6(l)): pins the CANDIDATE side of the vote surface against an
 * UNBOUND sender identity (a {@link NodeConnectionOutbound} whose remoteNodeId was never set by
 * onIntroduction).
 *
 * <p>On the live wire this is unreachable — {@code NodeConnectionOutboundImpl.receive} throws
 * before any non-bootstrap message from an unbound connection, and onIntroduction is the only
 * binder. These pins close the direct-handler surface (tests, middleware, future dispatch paths)
 * where the guard is the only line of defense:
 *
 * <ul>
 *   <li><b>Counting vector</b> — {@code votes.add(null)} is legal for a HashSet: one phoney
 *       unattributed "grant" would push the candidate to the majority and an illegitimate
 *       {@code convertToLeader}.</li>
 *   <li><b>Step-down vector</b> — a higher-term vote response from a null sender would
 *       {@code convertToFollower} (free term recognition for an unattributable voter).</li>
 *   <li><b>Vote-request vector</b> — a higher-term vote request from a null sender would
 *       {@code convertToFollowerForVote} and silently re-dispatch itself.</li>
 * </ul>
 *
 * <p>Every refusal takes the identity-family shape: warn, zero state change, NO reply — and the
 * discriminating bound-sender leg in each test proves the underlying legitimate path is not
 * burned by the refusal.
 */
class RaftNodeBehaviorCandidateVoteTest {
    private static final int TERM = 1;
    private static final long OWN_LAST_INDEX = 5L;

    /** Convenience for the messages-package TermIndex (clashes with the package-level one). */
    private static com.github.jocull.raftcache.lib.raft.messages.TermIndex ti(int term, long index) {
        return new com.github.jocull.raftcache.lib.raft.messages.TermIndex(term, index);
    }

    private static void appendLog(RaftNodeImpl node, long count) {
        for (long i = 1; i <= count; i++) {
            node.logs.appendLog(TERM, "entry-" + i);
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

    private static void closeCurrent(RaftNodeImpl node) {
        // Cancel whatever the current behavior has scheduled (the candidate's election timeout,
        // a leader heartbeat) so it cannot interleave further mutations after we assert.
        ((RaftNodeBehavior) node.currentReceiver()).close();
    }

    /**
     * A deterministic CANDIDATE of {@link #TERM} with two peers (cluster 3, majority 2): it
     * holds its own vote at construction, so ONE real peer grant completes the majority — and
     * without the null-sender guard, a single phoney unattributed grant would do the same.
     * Cannot self-elect, so every role change observed below is attributable to the driven
     * dispatches alone.
     */
    private static RaftNodeImpl newCandidate(PassThruMiddleware middleware, RaftManager raftManager) {
        final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
        middleware.addNode(node);
        appendLog(node, OWN_LAST_INDEX);
        registerPeer(node, "peerA");
        registerPeer(node, "peerB");
        node.start().join();
        node.convertToCandidate(TERM); // deterministic CANDIDATE of term 1
        assertEquals(NodeStates.CANDIDATE, stateOf(node));
        assertEquals(TERM, termOf(node));
        return node;
    }

    // ---- Counting vector: a null-sender "grant" must not count toward the majority ----

    @Test
    void unboundSender_onVoteResponse_grantNotCountedTowardMajority() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newCandidate(middleware, raftManager);

            // A phoney unattributed grant: without the guard, votes.add(null) is legal and this
            // single fake grant (own vote + null = 2 of 2) would ALONE complete the majority.
            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onVoteResponse(unbound, new VoteResponse(TERM, ti(TERM, 0L), true));

            // Identity-family shape: no reply, no counting — the candidate must still need a
            // real peer grant. (Without the guard it would have converted to leader HERE, at
            // the null-sender response — so this assertion is the discriminator.)
            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.CANDIDATE, stateOf(node), "an unattributed grant must not count toward the majority");

            // The legitimate path is alive: one REAL grant (own vote + peerA = 2 of 2) reaches
            // the majority and steps the candidate up — proving the refusal did not latch the
            // handler or corrupt the tally.
            final RecordingSender peerA = new RecordingSender("peerA");
            node.currentReceiver().onVoteResponse(peerA, new VoteResponse(TERM, ti(TERM, 0L), true));
            assertEquals(NodeStates.LEADER, stateOf(node),
                    "own vote plus one real grant reaches the majority and steps the candidate up");
            closeCurrent(node);
        }
    }

    // ---- Step-down vector: a higher-term null-sender response must not convert us ----

    @Test
    void unboundSender_onVoteResponse_higherTerm_doesNotStepDown() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newCandidate(middleware, raftManager);

            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onVoteResponse(unbound, new VoteResponse(TERM + 1, ti(TERM + 1, 0L), false));

            // The null sender must not earn even a step-down (convertToFollower at TERM + 1).
            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.CANDIDATE, stateOf(node), "a null-sender response must not step the candidate down");
            assertEquals(TERM, termOf(node), "a null-sender response must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the step-down path is not burned — a BOUND higher-term
            // response still converts the candidate to a follower of that term.
            final RecordingSender bound = new RecordingSender("boundVoter");
            node.currentReceiver().onVoteResponse(bound, new VoteResponse(TERM + 1, ti(TERM + 1, 0L), false));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a bound higher-term response still steps the candidate down");
            assertEquals(TERM + 1, termOf(node), "a bound higher-term response still advances the term");
            closeCurrent(node);
        }
    }

    // ---- Vote-request vector: a higher-term null-sender request must not convert us ----

    @Test
    void unboundSender_onVoteRequest_higherTerm_doesNotConvertToFollower() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newCandidate(middleware, raftManager);

            // Without the guard this would convertToFollowerForVote(TERM + 1) and then
            // re-dispatch itself into the fresh follower's voter path (grant, vote burn, reply).
            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onVoteRequest(unbound, new VoteRequest(TERM + 1, "unbound", ti(TERM + 1, OWN_LAST_INDEX)));

            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.CANDIDATE, stateOf(node), "a null-sender request must not convert the candidate to follower");
            assertEquals(TERM, termOf(node), "a null-sender request must not advance the term (no conversion)");
            closeCurrent(node);
        }
    }

    /**
     * A minimal {@link NodeConnectionOutbound} that identifies itself as a given voter and
     * records the {@link VoteResponse}s it is asked to send. Everything else is inert; the
     * identity-family refusal is expected to send NOTHING, so an empty record is the assertion.
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

        List<VoteResponse> voteResponses() {
            return List.copyOf(voteResponses);
        }
    }
}
