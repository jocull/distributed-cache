package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.data.PartitionMap;
import com.github.jocull.raftcache.lib.data.Snapshot;
import com.github.jocull.raftcache.lib.data.DataVersion;
import com.github.jocull.raftcache.lib.data.StateMachineBridge;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the follower side of {@link RaftNodeBehaviorFollower#onInstallSnapshot} (Raft §7,
 * SNAPSHOTS.md stage 4): the term ladder (higher term steps down first, lower term is
 * refused), the strict S2 progress-only acceptance, the adopt-before-compact ordering with
 * the bridge re-anchoring the applied position (S3/S4), and the reply carrying the new log
 * tail so the leader can resume {@link AppendEntries} past the boundary.
 *
 * <p>A started follower is term 0, so the equal-term cases drive term-0 messages; the
 * higher-term case enters through a term-7 message and the lower-term case first moves the
 * node to term 5. The state machine is a {@link StateMachineBridge} subscribed to the node's
 * own event bus -- the same wiring as production.
 */
class RaftNodeBehaviorFollowerInstallSnapshotTest {

    private final PartitionMap partitionMap = new PartitionMap(new EventBus(), 4);
    private StateMachineBridge bridge;

    private static DataVersion value(String v) {
        return value(v, 7L);
    }

    private static DataVersion value(String v, long version) {
        return new DataVersion(Map.of("value", v.getBytes(StandardCharsets.UTF_8)), version);
    }

    private void wireBridge(RaftNodeImpl node) {
        bridge = new StateMachineBridge(node.eventBus, partitionMap);
    }

    /** Install the standard term-0, boundary-(5,10), state {key1 -> v} snapshot. */
    private InstallSnapshot installMessage(RecordingSender leader, String stateValue) {
        final Snapshot snapshot = new Snapshot(5L, 10L, Map.of("key1", value(stateValue)));
        return new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, snapshot, true);
    }

    private static com.github.jocull.raftcache.lib.raft.messages.TermIndex wire(long term, long index) {
        return new com.github.jocull.raftcache.lib.raft.messages.TermIndex((int) term, index);
    }

    @Test
    void equalTermSnapshotBeyondTheTail_IsInstalledAndRepliesWithTheNewTail() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);

            assertEquals(0, ((RaftNodeBehavior) follower.currentReceiver()).term, "sanity: a started follower is term 0");
            follower.start().join();
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "sanity: fresh log is at the epoch");

            final RecordingSender leader = new RecordingSender("leader");
            follower.currentReceiver().onInstallSnapshot(leader, installMessage(leader, "value1"));

            // THE CORE: the reply (h) carries the follower's own term and the new tail --
            // the boundary, since the full install replaced the whole log.
            assertEquals(1, leader.snapshotResponses().size(), "exactly one reply");
            assertEquals(0, leader.snapshotResponses().get(0).getTerm(), "the reply carries the follower's own term");
            assertEquals(10L, leader.snapshotResponses().get(0).getIndex(), "the reply carries the new log tail");

            // S3: the log is re-anchored on the snapshot boundary, with the payload held as the
            // start point (the full install makes the boundary the recorded tail).
            assertTrue(follower.logs.hasStartPointSnapshot(), "the log must be anchored on the snapshot");
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertTrue(follower.currentReceiver() instanceof RaftNodeBehaviorFollower, "sanity: still a follower after the install");
            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex(), "after a full install the boundary is the tail");

            // The sender is recorded as this term's leader so its later heartbeats are accepted.
            final RaftNodeBehaviorFollower receiver = (RaftNodeBehaviorFollower) follower.currentReceiver();
            assertEquals("leader", receiver.getLeaderId());

            // S4 ordering: the state machine adopted the payload (publish is synchronous, so this
            // is observable right after onInstallSnapshot returns) with the applied position
            // re-anchored to the boundary.
            final Snapshot installedSnapshot = (Snapshot) follower.logs.getSnapshotPayload();
            assertEquals(5L, installedSnapshot.term(), "the stored payload is the snapshot we installed");
            assertEquals(10L, installedSnapshot.index());
            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                    installedSnapshot.entry("key1").getData().get("value"));
            assertTrue(partitionMap.getData("key1").isPresent(), "the adopted state must be readable");
            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
            assertEquals(7L, partitionMap.getData("key1").orElseThrow().getVersion(), "the captured version counter is restored");
            assertEquals(10L, bridge.getLastAppliedIndex(), "the applied position is re-anchored to the boundary");
            assertEquals(5L, bridge.getLastAppliedTerm());
        }
    }

    @Test
    void snapshotAtOrBehindTheTail_IsRefusedWithoutTouchingState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");

            // Install: boundary (5,10) strictly beyond the epoch.
            follower.currentReceiver().onInstallSnapshot(leader, installMessage(leader, "installed"));
            assertTrue(follower.logs.hasStartPointSnapshot(), "setup: the snapshot is installed");
            final long installedVersion = partitionMap.getData("key1").orElseThrow().getVersion();

            // S2: a snapshot whose boundary is at or behind our tail makes no progress, so it
            // must be refused -- even one carrying a wholly different payload.
            final Snapshot stale = new Snapshot(5L, 8L, Map.of("key1", value("stale-state", 9L)));
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 8L, 5, 0L, stale, true));

            assertEquals(2, leader.snapshotResponses().size(), "the refusal is answered, not dropped");
            assertEquals(0, leader.snapshotResponses().get(1).getTerm());
            assertEquals(10L, leader.snapshotResponses().get(1).getIndex(), "the refusal carries our tail, not the refused boundary");

            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex(), "a refused install must not move the tail");
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex(), "a refused install must not move the start point");
            assertEquals(installedVersion, partitionMap.getData("key1").orElseThrow().getVersion(),
                    "a refused install must not re-stamp the state");
            assertArrayEquals("installed".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"),
                    "a refused install must not replace the state");
        }
    }

    @Test
    void redeliveringAnAlreadyInstalledSnapshot_IsANoOp() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            follower.currentReceiver().onInstallSnapshot(leader, installMessage(leader, "installed"));
            final long installedVersion = partitionMap.getData("key1").orElseThrow().getVersion();

            // Re-delivery (the leader's log of the same install arrives again) has no index
            // beyond our boundary, so it is a no-op: answered, with the state untouched.
            final RecordingSender redelivery = new RecordingSender("redelivery");
            final Snapshot redelivered = new Snapshot(5L, 10L, Map.of("key1", value("other-state", 9L)));
            follower.currentReceiver().onInstallSnapshot(redelivery,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, redelivered, true));

            assertEquals(1, redelivery.snapshotResponses().size(), "the redelivery is refused, not re-applied");
            assertEquals(0, redelivery.snapshotResponses().get(0).getTerm());
            assertEquals(10L, redelivery.snapshotResponses().get(0).getIndex(), "the reply still carries our (unchanged) tail");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "a redelivery at the boundary must not record a different leader");
            assertEquals(installedVersion, partitionMap.getData("key1").orElseThrow().getVersion(),
                    "redelivery at the boundary must not re-install a different state");
        }
    }

    @Test
    void higherTermSnapshot_ConvertsToFollowerOfThatTerm_ThenInstalls() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final Snapshot snapshot = new Snapshot(9L, 20L, Map.of("key1", value("term9-state")));
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(7, leader.getRemoteNodeId(), 20L, 9, 0L, snapshot, true));

            // Stepped down to the leader's term first, with the sender recorded as leader.
            assertEquals(7, ((RaftNodeBehavior) follower.currentReceiver()).term, "the follower adopts the higher term");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId());

            // Then installed at the (now equal) term: new tail, boundary, adopted state.
            assertEquals(1, leader.snapshotResponses().size());
            assertEquals(7, leader.snapshotResponses().get(0).getTerm(), "the reply carries the new term");
            assertEquals(20L, leader.snapshotResponses().get(0).getIndex(), "the reply carries the new tail");
            assertEquals(20L, follower.logs.getCurrentTermIndex().getIndex());
            assertEquals(new TermIndex(9, 20L), follower.logs.getStartTermIndex());
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(),
                    "an install re-anchors the log but does not commit anything on its own");
            assertEquals(20L, bridge.getLastAppliedIndex());
            assertEquals(9L, bridge.getLastAppliedTerm());
            assertTrue(partitionMap.getData("key1").isPresent());
        }
    }

    @Test
    void lowerTermSnapshot_IsRefusedWithTheFollowerOwnTerm() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Move the follower to term 5 with an install.
            final Snapshot t5 = new Snapshot(5L, 10L, Map.of("key1", value("term5-state")));
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(5, leader.getRemoteNodeId(), 10L, 5, 0L, t5, true));
            assertEquals(5, ((RaftNodeBehavior) follower.currentReceiver()).term, "setup: the follower is at term 5");
            final long installedVersion = partitionMap.getData("key1").orElseThrow().getVersion();

            // A stale leader at term 3 arrives with a (smaller) snapshot: refuse, report OUR
            // term so it steps down, and change nothing.
            final RecordingSender staleLeader = new RecordingSender("staleLeader");
            final Snapshot stale = new Snapshot(2L, 4L, Map.of("key1", value("stale-state", 9L)));
            follower.currentReceiver().onInstallSnapshot(staleLeader,
                    new InstallSnapshot(3, staleLeader.getRemoteNodeId(), 4L, 2, 0L, stale, true));

            assertEquals(1, staleLeader.snapshotResponses().size(), "the stale leader must be answered");
            assertEquals(5, staleLeader.snapshotResponses().get(0).getTerm(), "the reply carries our own (higher) term");
            assertEquals(10L, staleLeader.snapshotResponses().get(0).getIndex(), "the reply carries our tail");

            assertEquals(5, ((RaftNodeBehavior) follower.currentReceiver()).term, "a stale snapshot must not move us back");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "a stale leader must not displace the recorded one");
            assertEquals(installedVersion, partitionMap.getData("key1").orElseThrow().getVersion(),
                    "a refused snapshot must not touch the state");
            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex());
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
        }
    }

    @Test
    void appendEntriesPointingInsideTheCompactedRegion_RestOnTheSnapshotBoundary() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            follower.currentReceiver().onInstallSnapshot(leader, installMessage(leader, "installed"));

            // Escape hatch: a previous-log pointer inside the compacted region is no longer
            // present in the log. The follower answers (without crashing and without
            // pretending to contain it) so the leader can fall back to InstallSnapshot.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(5, 7), wire(5, 7), List.of()));
            assertEquals(1, leader.acknowledges().size(), "an in-region append must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "entries we no longer hold must be rejected");
            assertEquals(10L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our true (boundary) tail");
            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");

            // And Raft §7's recovery: appending anchored AT the boundary (the leader sets
            // nextIndex to it after the install) is accepted and lands behind it.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(5, 10), wire(5, 11), List.of(new AppendEntries.RaftLog(wire(5, 11), "entry-11"))));
            assertEquals(2, leader.acknowledges().size());
            assertTrue(leader.acknowledges().get(1).isSuccess(), "an append at the boundary must be accepted");
            assertEquals(11L, follower.logs.getCurrentTermIndex().getIndex(), "the new entry lands behind the boundary");
            assertEquals(11L, follower.logs.getCommittedTermIndex().getIndex(),
                    "leaderCommit advanced the commit index to the appended entry");
            assertEquals(11L, bridge.getLastAppliedIndex(), "the committed entry was applied by the bridge");
        }
    }

    @Test
    void unstartedNode_RejectsInstallSnapshot() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(node);
            // Deliberately NOT started: the initial follower has no state to install into.
            assertThrows(IllegalStateException.class,
                    () -> node.currentReceiver().onInstallSnapshot(new RecordingSender("leader"),
                            new InstallSnapshot(1, "leader", 10L, 1, 0L, new Snapshot(1L, 10L, Map.of("key1", value("v"))), true)));
        }
    }

    /**
     * A minimal {@link NodeConnectionOutbound} that identifies itself as the sender and
     * records the {@link InstallSnapshotResponse}/{@link AcknowledgeEntries} it is asked to
     * send. Everything else is inert (mirrors the voter test's RecordingSender).
     */
    private static final class RecordingSender implements NodeConnectionOutbound {
        private final String remoteNodeId;
        private final List<InstallSnapshotResponse> snapshotResponses = new ArrayList<>();
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
            snapshotResponses.add(installSnapshotResponse);
        }

        List<InstallSnapshotResponse> snapshotResponses() {
            return List.copyOf(snapshotResponses);
        }

        List<AcknowledgeEntries> acknowledges() {
            return List.copyOf(acknowledges);
        }
    }
}
