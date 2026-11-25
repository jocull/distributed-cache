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
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 6(l) (SNAPSHOTS.md stage 6(l)): pins the LEADER side of the vote surface against an
 * UNBOUND sender identity (a {@link NodeConnectionOutbound} whose remoteNodeId was never set by
 * onIntroduction).
 *
 * <p>On the live wire this is unreachable — {@code NodeConnectionOutboundImpl.receive} throws
 * before any non-bootstrap message from an unbound connection. These pins close the
 * direct-handler surface (tests, middleware, future dispatch paths):
 *
 * <ul>
 *   <li><b>Vote request</b> — a higher-term request from a null sender would
 *       {@code convertToFollowerForVote} and then re-dispatch itself into the fresh follower,
 *       silently stepping a live leader down (and granting) on behalf of an unattributable voter.</li>
 *   <li><b>Vote response</b> — a higher-term response from a null sender would
 *       {@code convertToFollower} (free term recognition for an unattributable voter).</li>
 * </ul>
 *
 * <p>Every refusal takes the identity-family shape: warn, zero state change, NO reply — and the
 * discriminating bound-sender leg proves the legitimate higher-term step-down path is not burned.
 */
class RaftNodeBehaviorLeaderVoteTest {
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
        // Cancel whatever the current behavior has scheduled (the leader's heartbeat, a
        // follower's election timer) so it cannot interleave further mutations after we assert.
        ((RaftNodeBehavior) node.currentReceiver()).close();
    }

    /**
     * A deterministic LEADER of {@link #TERM}: two peers so the candidate phase cannot
     * self-elect, then a direct step-up to leader exactly as
     * {@link RaftNodeBehaviorPreVoteTest} builds its leader voters.
     */
    private static RaftNodeImpl newLeader(PassThruMiddleware middleware, RaftManager raftManager) {
        final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
        middleware.addNode(node);
        appendLog(node, OWN_LAST_INDEX);
        registerPeer(node, "peerA");
        registerPeer(node, "peerB");
        node.start().join();
        node.convertToCandidate(TERM);
        node.convertToLeader(); // deterministic LEADER of term 1
        assertEquals(NodeStates.LEADER, stateOf(node));
        assertEquals(TERM, termOf(node));
        return node;
    }

    // ---- Vote-request vector: a higher-term null-sender request must not step us down ----

    @Test
    void unboundSender_onVoteRequest_higherTerm_doesNotStepDown() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(middleware, raftManager);

            // Without the guard this would convertToFollowerForVote(TERM + 1) and then
            // re-dispatch itself: the null sender would be GRANTED as the unattributable voter
            // of the fresh follower, plus a reply it must never get.
            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onVoteRequest(unbound, new VoteRequest(TERM + 1, "unbound", ti(TERM + 1, OWN_LAST_INDEX)));

            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.LEADER, stateOf(node), "a null-sender request must not step a live leader down");
            assertEquals(TERM, termOf(node), "a null-sender request must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the legitimate path is alive — a BOUND higher-term request still
            // steps the candidate-follower path down and grants per the up-to-date rule.
            final RecordingSender bound = new RecordingSender("boundCandidate");
            node.currentReceiver().onVoteRequest(bound, new VoteRequest(TERM + 1, "boundCandidate", ti(TERM + 1, OWN_LAST_INDEX)));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a bound higher-term request still steps the leader down to follower");
            assertEquals(TERM + 1, termOf(node), "a bound higher-term request still advances the term");
            closeCurrent(node);
        }
    }

    // ---- Vote-response vector: a higher-term null-sender response must not step us down ----

    @Test
    void unboundSender_onVoteResponse_higherTerm_doesNotStepDown() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(middleware, raftManager);

            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onVoteResponse(unbound, new VoteResponse(TERM + 1, ti(TERM + 1, 0L), false));

            // The null sender must not earn even a step-down (convertToFollower at TERM + 1).
            assertTrue(unbound.voteResponses().isEmpty(), "an unbound sender must be refused without a reply");
            assertEquals(NodeStates.LEADER, stateOf(node), "a null-sender response must not step a live leader down");
            assertEquals(TERM, termOf(node), "a null-sender response must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the step-down path is not burned — a BOUND higher-term
            // response still converts the leader to a follower of that term.
            final RecordingSender bound = new RecordingSender("boundVoter");
            node.currentReceiver().onVoteResponse(bound, new VoteResponse(TERM + 1, ti(TERM + 1, 0L), false));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a bound higher-term response still steps the leader down");
            assertEquals(TERM + 1, termOf(node), "a bound higher-term response still advances the term");
            closeCurrent(node);
        }
    }

    // ---- Append-entries vector (stage 6(m)): a higher-term unbound append must not step us down ----

    @Test
    void unboundSender_appendEntries_higherTerm_doesNotStepDown() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(middleware, raftManager);

            // Without the guard this would convertToFollowerForNewLeader(TERM + 1) with a NULL
            // leader (the conversion — term advance, rollback, behavior replacement — happens
            // before the re-dispatch even reaches the guarded follower handler), and the
            // null-leader follower would then append and send a true ack to the unattributable
            // carrier: a live leader stepped down for an unauthenticated message.
            final RecordingSender unbound = new RecordingSender(null);
            node.currentReceiver().onAppendEntries(unbound,
                    new AppendEntries(TERM + 1, ti(0, 0L), ti(0, 0L), List.of()));

            assertTrue(unbound.acknowledges().isEmpty(), "an unbound sender must be refused without acknowledging");
            assertEquals(NodeStates.LEADER, stateOf(node), "an unbound higher-term append must not step a live leader down");
            assertEquals(TERM, termOf(node), "an unbound higher-term append must not advance the term (no conversion)");

            // THE DISCRIMINATOR: the legitimate path is alive — the SAME message from a BOUND
            // sender still steps the leader down to a follower of that term under that leader.
            // (The conversion rolls back our uncommitted term-1 tail, so the accepted
            // prev/commit pair is the fresh epoch tail — exactly what a legitimate leader
            // re-syncs to.)
            final RecordingSender bound = new RecordingSender("boundLeader");
            node.currentReceiver().onAppendEntries(bound,
                    new AppendEntries(TERM + 1, ti(0, 0L), ti(0, 0L), List.of()));
            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a bound higher-term append still steps the leader down to follower");
            assertEquals(TERM + 1, termOf(node), "a bound higher-term append still advances the term");
            assertEquals("boundLeader", ((RaftNodeBehaviorFollower) node.currentReceiver()).getLeaderId(), "the bound sender is recorded as leader");
            assertEquals(1, bound.acknowledges().size(), "the bound heartbeat at our tail must be acknowledged");
            assertTrue(bound.acknowledges().get(0).isSuccess(), "the bound heartbeat must be accepted");
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
        private final List<AcknowledgeEntries> acknowledges = new ArrayList<>();

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
            acknowledges.add(acknowledgeEntries);
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

        List<AcknowledgeEntries> acknowledges() {
            return List.copyOf(acknowledges);
        }
    }
}
