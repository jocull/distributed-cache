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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        return new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, chunk(snapshot), true);
    }

    /**
     * The chunk-protocol form of a capture (SNAPSHOTS.md stage 6a): the leader JDK-serializes
     * the supplier payload ONCE and the follower receives it as byte[] chunks, so every test
     * feed below goes through the same serialization the leader performs.
     */
    private static byte[] chunk(Snapshot snapshot) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(snapshot);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize test snapshot", e);
        }
        return out.toByteArray();
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
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 8L, 5, 0L, chunk(stale), true));

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
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, chunk(redelivered), true));

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
                    new InstallSnapshot(7, leader.getRemoteNodeId(), 20L, 9, 0L, chunk(snapshot), true));

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
                    new InstallSnapshot(5, leader.getRemoteNodeId(), 10L, 5, 0L, chunk(t5), true));
            assertEquals(5, ((RaftNodeBehavior) follower.currentReceiver()).term, "setup: the follower is at term 5");
            final long installedVersion = partitionMap.getData("key1").orElseThrow().getVersion();

            // A stale leader at term 3 arrives with a (smaller) snapshot: refuse, report OUR
            // term so it steps down, and change nothing.
            final RecordingSender staleLeader = new RecordingSender("staleLeader");
            final Snapshot stale = new Snapshot(2L, 4L, Map.of("key1", value("stale-state", 9L)));
            follower.currentReceiver().onInstallSnapshot(staleLeader,
                    new InstallSnapshot(3, staleLeader.getRemoteNodeId(), 4L, 2, 0L, chunk(stale), true));

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

    // ---- Stage 6h: hostile wire term/indexes on the append path are refused before any mutation ----

    /**
     * Stage 6h: the accept path feeds the append's wire axes -- each entry's term/index,
     * the previous-log pointer, and the leader's commit index -- straight into
     * {@code new TermIndex(...)}, whose constructor throws for a negative term or index
     * (the stage 6(g) root mechanism, this time on the replication path). None of those
     * axes is validity-checked first, so a hostile or corrupted append carrying one would
     * throw mid-handler AFTER the sender was already recorded as leader (and, for the
     * entries and the commit index, after some entries are already appended) -- the
     * "throw after mutation" class stage 6(g) closed on the snapshot path. A missing
     * field is hostile too: it NPEs out of the wire getters first. The refusal below must
     * therefore precede ALL of that: one false ack carrying our own tail, zero state
     * change. These pins keep the previous-log pointer valid (the epoch, which a fresh
     * log holds) so each one isolates exactly the axis being refused.
     */
    @Test
    void appendCarryingANegativeEntryIndex_isRefusedBeforeAnyMutation() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Without the guard this append sails past containsStartPoint (the epoch IS
            // held), records the sender as leader, and THEN throws IAE out of the entry's
            // term-index. The pin asserts that never happens: a clean refusal instead.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(0, 0), wire(0, 0),
                            List.of(new AppendEntries.RaftLog(wire(0, -1), "hostile-entry"))));

            assertEquals(1, leader.acknowledges().size(), "an append carrying a hostile index must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "structurally invalid entries must be rejected");
            assertEquals(0L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our unchanged (epoch) tail");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "an append we refuse must not install a leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(), "the commit point is untouched by the refusal");
            assertEquals(0L, bridge.getLastAppliedIndex(), "nothing is applied to the state machine");
        }
    }

    @Test
    void appendCarryingANegativeLeaderCommitIndex_isRefusedBeforeAnyMutation() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // The commit-alignment axis: without the guard the append would be accepted
            // (no entries to append) and the IAE would only fire when the commit advanced,
            // again after the leaderId mutation.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(0, 0), wire(0, -1), List.of()));

            assertEquals(1, leader.acknowledges().size(), "an append carrying a hostile commit index must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "a structurally invalid commit index must be rejected");
            assertEquals(0L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our unchanged (epoch) tail");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "an append we refuse must not install a leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(), "the commit point is untouched by the refusal");
        }
    }

    @Test
    void appendCarryingANullEntryTermIndex_isRefusedWithoutThrowing() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // A wire field is absent (null): equally hostile -- without the guard it would
            // NPE out of the getters on the accept path, again after the leaderId mutation.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(0, 0), wire(0, 0),
                            List.of(new AppendEntries.RaftLog(null, "hostile-entry"))));

            assertEquals(1, leader.acknowledges().size(), "an append carrying a missing term/index must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "a structurally invalid entry must be rejected");
            assertEquals(0L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our unchanged (epoch) tail");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "an append we refuse must not install a leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(), "the commit point is untouched by the refusal");
        }
    }

    // ---- Stage 6j: a null entry slot in the entries list is refused, not thrown on ----

    /**
     * Stage 6j: one more structurally invalid shape than the 6(h) pins -- a null SLOT
     * inside the entries list (the 6(h) pins cover a null term/index INSIDE a well-formed
     * entry). The wire list admits such slots: {@code AppendEntries} copies it without
     * element checks (unlike {@code AnnounceClusterTopology}'s {@code List.copyOf}, which
     * rejects them at construction). A null slot would NPE the very dereference this
     * refusal performs while classifying the entries -- the guard throwing where it is
     * supposed to catch. The refusal keeps the 6(h) shape: exactly one false ack with our
     * unchanged tail, zero state change, no leader installed.
     */
    @Test
    void appendCarryingANullEntrySlot_isRefusedWithoutThrowing() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Null slot first, then a well-formed entry: without the slot check the guard
            // NPEs on the first classification pass instead of refusing the append.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(0, 0), wire(0, 0),
                            Arrays.asList((AppendEntries.RaftLog) null, new AppendEntries.RaftLog(wire(0, 1), "ok-entry"))));

            assertEquals(1, leader.acknowledges().size(), "an append carrying a null entry slot must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "an append with a null slot must be rejected");
            assertEquals(0L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our unchanged (epoch) tail");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "an append we refuse must not install a leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(), "the commit point is untouched by the refusal");
            assertEquals(0L, bridge.getLastAppliedIndex(), "nothing is applied to the state machine");
        }
    }

    @Test
    void appendCarryingANullTailEntrySlot_isRefusedWithoutThrowing() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Null slot at the tail: this is the slot the accept path reads FIRST as its
            // new tail (last entry's term/index) -- the "throw after mutation" surface the
            // 6(h) pins closed for the other axes. The refusal must precede that read.
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(0, wire(0, 0), wire(0, 0),
                            Arrays.asList(new AppendEntries.RaftLog(wire(0, 1), "ok-entry"), null)));

            assertEquals(1, leader.acknowledges().size(), "an append with a null tail slot must be answered");
            assertFalse(leader.acknowledges().get(0).isSuccess(), "an append with a null tail slot must be rejected");
            assertEquals(0L, leader.acknowledges().get(0).getCurrentTermIndex().getIndex(),
                    "the refusal carries our unchanged (epoch) tail");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "an append we refuse must not install a leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is untouched by the refusal");
            assertEquals(0L, follower.logs.getCommittedTermIndex().getIndex(), "the commit point is untouched by the refusal");
            assertEquals(0L, bridge.getLastAppliedIndex(), "nothing is applied to the state machine");
        }
    }

    // ---- Stage 6k: structurally invalid bootstrap traffic is refused before any mutation ----
    //
    // The bootstrap handlers (onIntroduction / onAnnounceClusterTopology, stage 6(k))
    // live in the RaftNodeBehavior base and, like the leader-ack paths the family
    // hardened after them, have no ack to carry a refusal -- so the refusal shape is the
    // no-reply one: warn, change ZERO state (no binding, no registration, no reply).
    // The carriers are bare, unlike the null-guards on the term/index wire types: a
    // missing id becomes a null map key (legal, but it inflates getMajorityCount() and
    // votes can never target it), and a missing address NPEs or throws out of the
    // FIRST locate() that follows -- the stage 6(j) "throw after mutation" mechanism,
    // this time on the bootstrap surface.

    @Test
    void anIntroductionCarryingANullNodeId_isRefusedBeforeBindingRegisteringOrReplying() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender peer = new RecordingSender("peer");
            // Without the guard this would bind the connection to a null id, register a
            // null key (quorum inflation) and THEN reply -- the mutation the guard must
            // precede.
            follower.currentReceiver().onIntroduction(peer,
                    new Introduction(null, new NodeAddress("peerAddress"), wire(0, 0)));

            assertTrue(peer.remoteNodeIdBindings().isEmpty(),
                    "a refused introduction must not bind the connection");
            assertEquals(1, follower.clusterTopology.getClusterCount(),
                    "a refused introduction registers nothing");
            assertEquals(1, follower.clusterTopology.getMajorityCount(),
                    "the quorum is not inflated by a null key");
            assertTrue(peer.announcedTopologies().isEmpty(),
                    "a refused introduction announces no topology");
        }
    }

    @Test
    void anIntroductionCarryingANullNodeAddress_isRefusedBeforeBindingRegisteringOrReplying() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender peer = new RecordingSender("peer");
            // Null address: register would accept it into the map, and the reply built
            // from getTopology() would carry the null-address entry out to peers -- the
            // 6(j) null-slot shape, on the bootstrap surface. The guard precedes both.
            follower.currentReceiver().onIntroduction(peer,
                    new Introduction("peer", null, wire(0, 0)));

            assertTrue(peer.remoteNodeIdBindings().isEmpty(),
                    "a refused introduction must not bind the connection");
            assertEquals(1, follower.clusterTopology.getClusterCount(),
                    "a refused introduction registers nothing");
            assertTrue(peer.announcedTopologies().isEmpty(),
                    "a refused introduction announces no topology");
        }
    }

    @Test
    void anAnnounceCarryingANullNodeIdEntry_isRefusedWithoutRegisteringAnything() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // The 6(i) whole-list convention: one bad entry refuses the WHOLE announce.
            // The null id is the quorum-inflation vector (null key); the "legit" entry
            // would have been fine on its own -- exactly why it must go down with the
            // bad one.
            follower.currentReceiver().onAnnounceClusterTopology(leader, new AnnounceClusterTopology(
                    List.of(
                            new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("legit", new NodeAddress("legitAddress")),
                            new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(null, new NodeAddress("hostileAddress")))));

            assertEquals(1, follower.clusterTopology.getClusterCount(),
                    "a refused announce registers nothing -- not even its valid entries");
            assertEquals(1, follower.clusterTopology.getMajorityCount(),
                    "the quorum is unchanged");
            assertTrue(leader.remoteNodeIdBindings().isEmpty(),
                    "a refused announce touches the sender's binding not at all");
            assertTrue(leader.announcedTopologies().isEmpty());
        }
    }

    @Test
    void anAnnounceCarryingANullNodeAddressEntry_fromAnUnboundSender_isRefusedWithoutThrowing() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender unbound = new RecordingSender(null); // still unbound on the stub (address "stub-unbound")
            // Without the guard this NPEs out of locate() AFTER the topology has ALREADY
            // been registered: the filter dereferences every entry's address, and the
            // null-address entry below is what it hits -- the throw-after-mutation shape.
            follower.currentReceiver().onAnnounceClusterTopology(unbound, new AnnounceClusterTopology(
                    List.of(
                            new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("peer", new NodeAddress("stub-unbound")),
                            new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("hostile", null))));

            assertEquals(1, follower.clusterTopology.getClusterCount(),
                    "the NPE-able entry is registered by nothing");
            assertTrue(unbound.remoteNodeIdBindings().isEmpty(),
                    "and the sender is never bound");
        }
    }

    @Test
    void anAnnounceFromAnUnboundSenderWhoIsNotInTheTopology_isRefusedWithoutRegistering() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender unbound = new RecordingSender(null); // address "stub-unbound"
            final AnnounceClusterTopology unresolvable = new AnnounceClusterTopology(
                    List.of(new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("peer", new NodeAddress("someOtherAddress"))));
            // The announce is well-formed but simply does not name the (unbound) sender
            // and we do not hold it: without the guard we would register the topology and
            // THEN throw the ISE out of locate() -- the throw-after-mutation shape, now
            // refused before any mutation instead.
            assertDoesNotThrow(
                    () -> follower.currentReceiver().onAnnounceClusterTopology(unbound, unresolvable),
                    "an unresolvable sender is refused, not thrown on after the topology moved");

            assertEquals(1, follower.clusterTopology.getClusterCount(),
                    "a refused announce registers nothing");
            assertEquals(1, follower.clusterTopology.getMajorityCount(),
                    "the quorum is unchanged");
            assertTrue(unbound.remoteNodeIdBindings().isEmpty(),
                    "and the sender is never bound");
        }
    }

    @Test
    void anIntroductionFromAMatchedPeer_bindsRegistersAndAnnounces() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender peer = new RecordingSender("peer");
            // The legitimate bootstrap the guard must keep working: a well-formed
            // introduction binds the connection, registers the peer, and replies with
            // the (now grown) topology.
            follower.currentReceiver().onIntroduction(peer,
                    new Introduction("peer", new NodeAddress("peerAddress"), wire(0, 0)));

            assertEquals(List.of("peer"), peer.remoteNodeIdBindings(),
                    "the connection is bound to the introducer");
            assertEquals(2, follower.clusterTopology.getClusterCount(),
                    "the introducer is registered");
            assertEquals(1, peer.announcedTopologies().size(),
                    "and the reply is one announce");
            final List<com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier> reply =
                    peer.announcedTopologies().get(0).getNodeIdentifierStates();
            assertEquals(2, reply.size(),
                    "the reply carries our own entry and the introducer's");
            assertTrue(reply.stream().anyMatch(i -> "follower".equals(i.getId())),
                    "self is still in the announced topology");
            assertTrue(reply.stream().anyMatch(i -> "peer".equals(i.getId())),
                    "the introducer is in the announced topology");
        }
    }

    @Test
    void anAnnounceFromAnUnboundSenderThatListsItself_resolvesBindsAndRegisters() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender unbound = new RecordingSender(null); // address "stub-unbound"
            // A real node always announces itself, so the sender's address is resolvable
            // INSIDE its own list -- the legitimate bootstrap this pin must keep
            // working: the guard may not over-refuse.
            final AnnounceClusterTopology announce = new AnnounceClusterTopology(
                    List.of(new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("peer", new NodeAddress("stub-unbound"))));
            follower.currentReceiver().onAnnounceClusterTopology(unbound, announce);

            assertEquals(List.of("peer"), unbound.remoteNodeIdBindings(),
                    "the sender is resolved from its own announce and bound");
            assertEquals(2, follower.clusterTopology.getClusterCount(),
                    "the announce is registered exactly once");
        }
    }

    // ---- Chunked transfer (stage 6a): byte[] chunks, byte cursor, in-order assembly ----

    @Test
    void snapshotArrivingInTwoChunks_isAssembledAndInstalledOnlyOnTheFinalChunk() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final byte[] data = chunk(new Snapshot(5L, 10L, Map.of("key1", value("chunked"))));
            final int split = data.length / 2;
            final byte[] first = Arrays.copyOfRange(data, 0, split);
            final byte[] second = Arrays.copyOfRange(data, split, data.length);

            // Chunk 1: buffered, NOT installed -- the reply reports the follower's cursor.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            assertEquals(1, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(0).getTerm());
            assertEquals(0L, leader.snapshotResponses().get(0).getIndex(), "an intermediate reply carries the (unchanged) tail");
            assertEquals(split, leader.snapshotResponses().get(0).getOffset(), "an intermediate reply reports the next expected byte");
            // Nothing may be installed mid-transfer.
            assertFalse(follower.logs.hasStartPointSnapshot(), "an incomplete transfer installs nothing");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the tail is untouched until the final chunk");
            assertTrue(partitionMap.getData("key1").isEmpty(), "the state machine must not see partial state");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "chunk 0 is the accept path, so the sender is recorded as leader");

            // Chunk 2: completes the buffer -- install lands exactly as a single-chunk install.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split, second, true));
            assertEquals(2, leader.snapshotResponses().size());
            assertEquals(10L, leader.snapshotResponses().get(1).getIndex(), "the completion reply carries the new tail");
            assertEquals((long) data.length, leader.snapshotResponses().get(1).getOffset(), "the completion reply reports the total bytes");

            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex());
            assertArrayEquals("chunked".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
            assertEquals(10L, bridge.getLastAppliedIndex(), "the applied position is re-anchored to the boundary");
        }
    }

    @Test
    void resendingAnAlreadyAppendedChunk_isDroppedWithTheCursorReReported() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final byte[] data = chunk(new Snapshot(5L, 10L, Map.of("key1", value("dup"))));
            final int split = data.length / 2;
            final byte[] first = Arrays.copyOfRange(data, 0, split);
            final byte[] second = Arrays.copyOfRange(data, split, data.length);

            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));

            // Lost-ack retransmit: chunk 1 arrives AGAIN. It must be dropped (not buffered
            // twice) and the reply must re-report our cursor so the leader resumes PAST it.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            assertEquals(2, leader.snapshotResponses().size(), "a duplicate is answered, not silently dropped");
            assertEquals(split, leader.snapshotResponses().get(1).getOffset(),
                    "the duplicate reply re-reports the unchanged cursor");

            // The final chunk then lands exactly once -- a double-buffered first chunk would
            // make the assembled stream undecodable (or at least mis-sized), so a clean
            // install here proves the duplicate was discarded.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split, second, true));
            assertEquals(10L, leader.snapshotResponses().get(2).getIndex());
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertArrayEquals("dup".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
        }
    }

    @Test
    void aGapRefusesWithCursorZero_soTheTransferCanRestartFromByteZero() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final byte[] data = chunk(new Snapshot(5L, 10L, Map.of("key1", value("gap"))));
            final int split = data.length / 2;
            final byte[] first = Arrays.copyOfRange(data, 0, split);
            final byte[] second = Arrays.copyOfRange(data, split, data.length);

            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));

            // We are missing the bytes between our cursor and this chunk: refuse with cursor
            // 0, which tells the leader to restart the transfer from byte 0.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split + 1L, second, true));
            assertEquals(2, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(1).getOffset(), "a gap is answered with a 'restart from byte 0' cursor");
            assertFalse(follower.logs.hasStartPointSnapshot(), "a refused chunk installs nothing");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex());

            // The escape valve: the leader re-sends byte 0 and the transfer re-baselines
            // (offset 0 = fresh start) and runs to completion.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            assertEquals((long) split, leader.snapshotResponses().get(2).getOffset());
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split, second, true));
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertArrayEquals("gap".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
        }
    }

    @Test
    void aNegativeOffsetRefusesWithCursorZero_soTheTransferRestartsFromByteZero() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final byte[] data = chunk(new Snapshot(5L, 10L, Map.of("key1", value("neg"))));
            final int split = data.length / 2;
            final byte[] first = Arrays.copyOfRange(data, 0, split);

            // The transfer is in flight under (5,10): the offset-0 half is accepted.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            assertEquals((long) split, leader.snapshotResponses().get(0).getOffset(), "chunk 1 accepted; the cursor says 'send me byte split'");

            // A negative offset is not a byte position. It must NOT fall into the
            // "lost-ack retransmit" branch (which would re-report the in-flight cursor, so the
            // leader never learns its send was illegal) but be refused with cursor 0 and the
            // in-flight transfer torn down.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, -1L, first, true));
            assertEquals(2, leader.snapshotResponses().size(), "the illegal chunk is answered, not dropped");
            assertEquals(0, leader.snapshotResponses().get(1).getOffset(), "an illegal cursor is answered with a 'restart from byte 0' cursor");
            assertFalse(follower.logs.hasStartPointSnapshot(), "a refused chunk installs nothing");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "a refused chunk must not move the tail");
            assertTrue(partitionMap.getData("key1").isEmpty(), "a refused chunk must not touch the state");

            // The escape valve then runs: the leader re-sends byte 0 and the transfer
            // re-baselines to completion.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            assertEquals((long) split, leader.snapshotResponses().get(2).getOffset(), "the re-send of byte 0 re-baselines the transfer");
            final byte[] second = Arrays.copyOfRange(data, split, data.length);
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split, second, true));
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertArrayEquals("neg".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
        }
    }

    @Test
    void aChunkUnderADifferentBoundary_isRefusedWithCursorZero() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final byte[] data = chunk(new Snapshot(5L, 10L, Map.of("key1", value("boundary"))));
            final int split = data.length / 2;
            final byte[] first = Arrays.copyOfRange(data, 0, split);
            final byte[] second = Arrays.copyOfRange(data, split, data.length);

            // In flight under boundary (5,10): chunk 1 accepted.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));

            // The peer restarts under DIFFERENT capture coordinates (5,12): its cursor is
            // meaningless to us -- refuse so it re-baselines with a coherent capture.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 12L, 5, split, second, true));
            assertEquals(2, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(1).getOffset(), "a boundary mismatch is answered with 'restart from byte 0'");
            assertFalse(follower.logs.hasStartPointSnapshot(), "a mismatched chunk installs nothing");

            // ...and the coherent (5,10) capture still runs to completion.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, first, false));
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, split, second, true));
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertArrayEquals("boundary".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
        }
    }

    @Test
    void aNonByteArrayPayload_isRefusedWithoutTouchingState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Not a byte[] chunk: outside the chunk protocol. Refuse, answer the leader,
            // and change nothing.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, "not-a-chunk", true));

            assertEquals(1, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(0).getOffset());
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "a refused payload must not move the tail");
            assertFalse(follower.logs.hasStartPointSnapshot());
            assertTrue(partitionMap.getData("key1").isEmpty(), "a refused payload must not touch the state");
        }
    }

    @Test
    void aNullPayload_isBlockedAtTheMessageBoundary_soTheFollowerNeverSeesOne() {
        // The InstallSnapshot constructor null-guards the payload (Objects.requireNonNull),
        // so a null payload cannot be built or delivered today; the handler's null-payload
        // branch is defense-in-depth behind that invariant. This pin makes the invariant
        // test-visible, so a construction path that drops it cannot silently move the guard
        // from the message boundary down into the handler.
        assertThrows(NullPointerException.class,
                () -> new InstallSnapshot(0, "leader", 10L, 5, 0L, null, true),
                "the message boundary null-guards the payload");
    }

    @Test
    void aNegativeBoundaryTerm_isRefusedInTheSameShapeAsANegativeOffset() {
        // (6g) The accept path builds new TermIndex(lastIncludedTerm, lastIncludedIndex)
        // straight from wire values (the chunk-cursor section below the guard). The index axis
        // is structurally gated: S2 requires strictly beyond our tail, so it is already >= 0.
        // The term axis is NOT: a same-term peer with boundary term -1 would IAE out of the
        // TermIndex constructor AFTER the accept path had recorded that peer as leader. The
        // negative-offset sibling (SNAPSHOTS.md stage 6e) already refuses with the convention
        // offset-0 reply + re-armed election + zero state change; this pin demands the same
        // refusal fire BEFORE the acceptance, i.e. before ANY state change.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager
                    .newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // Boundary index 10 STRICTLY beyond this fresh follower's tail (0) so it passes S2;
            // boundary TERM -1 is the hostile axis; offset 0, done true, a legal byte[] payload,
            // so the handler would reach the accept path if unguarded and IAE mid-change.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, -1, 0L, new byte[]{(byte) 0xAA}, true));

            assertEquals(1, leader.snapshotResponses().size(), "a refused payload is answered, not silently dropped");
            assertEquals(0, leader.snapshotResponses().get(0).getOffset(),
                    "the refusal reports offset 0 -- 'start over' -- exactly like the negative-offset refusal");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "a refused payload must not move the tail");
            assertFalse(follower.logs.hasStartPointSnapshot(), "a refused payload must not leave a capture behind");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(),
                    "a refused payload must NOT have recorded the sender as leader -- the guard fires before the accept path");

            // And no half-transfer lingers to smuggle the next chunk through: resend the SAME
            // "transfer" at offset 1 and it must be refused too (offset 0), because nothing was
            // buffered to place it against.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, -1, 1L, new byte[]{(byte) 0xAB}, false));
            assertEquals(2, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(1).getOffset(),
                    "offset 1 of a refused transfer has no in-flight record to land on, so it too restarts from byte 0");
        }
    }

    @Test
    void anUndecodableAssembledCapture_isRefusedWithoutTouchingState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // The assembled buffer decodes into nothing: the transfer must fail OPEN-REFUSAL
            // (answer with cursor 0, leader retransmits from byte 0) -- never a half-install.
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, new byte[]{1, 2, 3}, true));

            assertEquals(1, leader.snapshotResponses().size());
            assertEquals(0, leader.snapshotResponses().get(0).getOffset(), "a decode failure is answered with 'restart from byte 0'");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "a decode failure must not move the tail");
            assertFalse(follower.logs.hasStartPointSnapshot());
            assertTrue(partitionMap.getData("key1").isEmpty());
            assertEquals(0L, bridge.getLastAppliedIndex(), "a decode failure must not advance the applied position");
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
                            new InstallSnapshot(1, "leader", 10L, 1, 0L, chunk(new Snapshot(1L, 10L, Map.of("key1", value("v")))), true)));
        }
    }

    // ---- SNAPSHOT STORE (stage 6b): a completed install persists the wire bytes ----

    @Test
    void installSnapshot_isPersistedToTheSnapshotStore() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            final InMemorySnapshotStore store = new InMemorySnapshotStore();
            follower.setSnapshotStore(store);
            wireBridge(follower);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            final Snapshot snapshot = new Snapshot(5L, 10L, Map.of("key1", value("value1")));
            final byte[] wire = chunk(snapshot);
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(0, leader.getRemoteNodeId(), 10L, 5, 0L, wire, true));

            // THE CORE (stage 6b): the completed install persisted the EXACT wire bytes under the
            // install boundary, before the log was re-anchored on it.
            assertTrue(follower.logs.hasStartPointSnapshot(), "sanity: the install completed and compacted");
            final SnapshotStore.Stored stored = store.loadLatest()
                    .orElseThrow(() -> new AssertionError("the completed install must persist its snapshot"));
            assertEquals(new TermIndex(5, 10L), stored.boundary(), "the boundary is the install boundary");
            assertArrayEquals(wire, stored.payload(), "the store holds the exact wire bytes we installed");

            // S4 is undisturbed: the state still adopted, applied position re-anchored.
            assertTrue(partitionMap.getData("key1").isPresent(), "the adopted state is readable");
            assertEquals(10L, bridge.getLastAppliedIndex(), "the applied position is re-anchored to the boundary");
        }
    }

    // ---- SNAPSHOT STORE (stage 6b): startup re-adopts a seeded snapshot ----

    @Test
    void startup_seedsAPersistedSnapshotIntoLogAndState() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final InMemorySnapshotStore store = new InMemorySnapshotStore();
            final Snapshot snapshot = new Snapshot(5L, 10L, Map.of("key1", value("value1")));
            store.save(new TermIndex(5, 10L), chunk(snapshot));

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            follower.setSnapshotStore(store);
            wireBridge(follower); // subscribed before start so it catches the seed's adopt event
            follower.start().join();

            // The log is anchored on the seeded snapshot, with the boundary as its recorded tail.
            assertTrue(follower.logs.hasStartPointSnapshot(), "the seed anchored the log on the snapshot");
            assertEquals(new TermIndex(5, 10L), follower.logs.getStartTermIndex());
            assertEquals(10L, follower.logs.getCurrentTermIndex().getIndex(), "the seeded boundary is the tail");

            // The re-anchored payload is the snapshot we seeded.
            final Snapshot installed = (Snapshot) follower.logs.getSnapshotPayload();
            assertEquals(5L, installed.term());
            assertEquals(10L, installed.index());

            // And the state was re-adopted through the bridge, exactly like a live install.
            assertTrue(partitionMap.getData("key1").isPresent(), "the seeded state is readable");
            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                    partitionMap.getData("key1").orElseThrow().getData().get("value"));
            assertEquals(7L, partitionMap.getData("key1").orElseThrow().getVersion(), "the captured version counter is restored");
            assertEquals(10L, bridge.getLastAppliedIndex(), "the applied position is re-anchored to the boundary");
            assertEquals(5L, bridge.getLastAppliedTerm());
        }
    }

    @Test
    void startup_aStoreThatFailsToLoad_doesNotPreventStartup() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {

            final SnapshotStore throwing = new SnapshotStore() {
                @Override
                public void save(TermIndex boundary, byte[] payload) {
                    // not used by the seed path
                }

                @Override
                public Optional<SnapshotStore.Stored> loadLatest() {
                    throw new IllegalStateException("simulated corrupt store");
                }
            };

            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            follower.setSnapshotStore(throwing);
            // A corrupt/malformed snapshot must never prevent startup (SNAPSHOTS.md): the seed
            // swallows the load failure and the node comes up on its (fresh) log regardless.
            follower.start().join();

            assertFalse(follower.logs.hasStartPointSnapshot(), "a failed seed installs no snapshot");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "the log is left untouched");
        }
    }

    @Test
    void finalChunkWithFailingSnapshotStore_failsClosed_stateNotAdopted_noReplySent() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            final SnapshotStore failing = new SnapshotStore() {
                @Override
                public void save(TermIndex boundary, byte[] payload) {
                    throw new IllegalStateException("simulated snapshot-store failure");
                }

                @Override
                public Optional<SnapshotStore.Stored> loadLatest() {
                    return Optional.empty();
                }
            };
            follower.setSnapshotStore(failing);
            follower.start().join();

            final RecordingSender leader = new RecordingSender("leader");
            // THE CORE (6c): the final chunk deserializes cleanly, then the store refuses to
            // persist it. Everything after the save in the S4 order (adopt, compact, reply)
            // must NOT happen -- and the error escapes instead of being swallowed, so the
            // leader never learns of a success it did not get.
            assertThrows(IllegalStateException.class,
                    () -> follower.currentReceiver().onInstallSnapshot(leader, installMessage(leader, "value1")),
                    "a failed save is a hard error, not a silent skip");

            assertTrue(leader.snapshotResponses().isEmpty(),
                    "the strongest signal: a failed install must NOT acknowledge success, so the leader retransmits");
            assertFalse(follower.logs.hasStartPointSnapshot(), "fail-safe: the log is not re-anchored on a failed install");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "fail-safe: the log is untouched");
            assertTrue(partitionMap.getData("key1").isEmpty(), "fail-safe: the state machine never adopted the payload");
            assertEquals(0L, bridge.getLastAppliedIndex(), "fail-safe: the applied position is not advanced");
        }
    }

    // ---- Stage 6(m): unbound-sender refusals on the replication and snapshot surfaces ----

    @Test
    void unboundSenderAppendEntries_higherTerm_doesNotConvert() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            // Stage 6(m): without the guard, this higher-term append converts us to a follower
            // of term 7 with a NULL leader (the conversion happens before the re-dispatch
            // reaches the guarded follower handler), then appends and sends a true ack to the
            // unattributable carrier -- free term recognition plus an acknowledged append.
            final RecordingSender unbound = new RecordingSender(null);
            follower.currentReceiver().onAppendEntries(unbound,
                    new AppendEntries(7, wire(0, 0L), wire(0, 0L), List.of()));

            assertTrue(unbound.acknowledges().isEmpty(), "an unbound sender must be refused without acknowledging");
            assertTrue(follower.currentReceiver() instanceof RaftNodeBehaviorFollower, "an unbound append must not convert the role");
            assertEquals(0, ((RaftNodeBehavior) follower.currentReceiver()).term, "an unbound append must not advance the term");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(), "an unbound append must not name anyone as leader");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "fail-safe: the log is untouched");

            // THE DISCRIMINATOR: the legitimate path is alive -- the SAME message from a BOUND
            // sender converts the term, names the leader, and is acknowledged.
            final RecordingSender leader = new RecordingSender("leader");
            follower.currentReceiver().onAppendEntries(leader,
                    new AppendEntries(7, wire(0, 0L), wire(0, 0L), List.of()));
            assertEquals(7, ((RaftNodeBehavior) follower.currentReceiver()).term, "a bound higher-term append must advance the term");
            assertTrue(follower.currentReceiver() instanceof RaftNodeBehaviorFollower, "a bound higher-term append must convert to follower");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(), "the bound sender is recorded as leader");
            assertEquals(1, leader.acknowledges().size(), "the bound append must be acknowledged");
            assertTrue(leader.acknowledges().get(0).isSuccess(), "the bound heartbeat at our tail must be accepted");
        }
    }

    @Test
    void unboundSenderInstallSnapshot_higherTerm_neitherConvertsNorInstalls() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl follower = (RaftNodeImpl) raftManager.newNode("follower", new NodeAddress("followerAddress"));
            middleware.addNode(follower);
            wireBridge(follower);
            follower.start().join();

            // Stage 6(m): without the guard, this higher-term snapshot first converts the node
            // to a follower of term 7 with a NULL leader, then installs the payload, re-anchors
            // the log and the state machine -- all on behalf of an unattributable leader. The
            // message SELF-CLAIMS a leader (the wire always names one); the guard the pin
            // exercises is the sender's identity, which the transport could not bind.
            final Snapshot snapshot = new Snapshot(9L, 20L, Map.of("key1", value("term9-state")));
            final RecordingSender unbound = new RecordingSender(null);
            follower.currentReceiver().onInstallSnapshot(unbound,
                    new InstallSnapshot(7, "claimedLeader", 20L, 9, 0L, chunk(snapshot), true));

            assertTrue(unbound.snapshotResponses().isEmpty(), "an unbound install must not acknowledge success");
            assertTrue(follower.currentReceiver() instanceof RaftNodeBehaviorFollower, "an unbound install must not convert the role");
            assertEquals(0, ((RaftNodeBehavior) follower.currentReceiver()).term, "an unbound install must not advance the term");
            assertNull(((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(), "an unbound install must not name anyone as leader");
            assertFalse(follower.logs.hasStartPointSnapshot(), "fail-safe: the log is not re-anchored on an unbound install");
            assertEquals(0L, follower.logs.getCurrentTermIndex().getIndex(), "fail-safe: the log is untouched");
            assertEquals(0L, bridge.getLastAppliedIndex(), "fail-safe: the applied position is not advanced");
            assertTrue(partitionMap.getData("key1").isEmpty(), "fail-safe: the state machine never adopted the payload");

            // THE DISCRIMINATOR: the legitimate path is alive -- the SAME snapshot from a BOUND
            // sender converts the term, names the leader, installs, and acknowledges.
            final RecordingSender leader = new RecordingSender("leader");
            follower.currentReceiver().onInstallSnapshot(leader,
                    new InstallSnapshot(7, leader.getRemoteNodeId(), 20L, 9, 0L, chunk(snapshot), true));
            assertEquals(7, ((RaftNodeBehavior) follower.currentReceiver()).term, "a bound higher-term install must advance the term");
            assertEquals("leader", ((RaftNodeBehaviorFollower) follower.currentReceiver()).getLeaderId(), "the bound sender is recorded as leader");
            assertEquals(1, leader.snapshotResponses().size(), "the bound install must be acknowledged");
            assertEquals(7, leader.snapshotResponses().get(0).getTerm(), "the reply carries the new term");
            assertEquals(20L, leader.snapshotResponses().get(0).getIndex(), "the reply carries the new tail");
            assertEquals(20L, follower.logs.getCurrentTermIndex().getIndex(), "the boundary is the new tail");
            assertEquals(new TermIndex(9, 20L), follower.logs.getStartTermIndex(), "the log is anchored on the boundary");
            assertEquals(20L, bridge.getLastAppliedIndex(), "the applied position is re-anchored");
            assertEquals(9L, bridge.getLastAppliedTerm(), "the applied term follows the boundary");
            assertTrue(partitionMap.getData("key1").isPresent(), "the state machine adopted the payload");
        }
    }

    /**
     * A minimal {@link NodeConnectionOutbound} that identifies itself as the sender and
     * RECORDS what it is told to do: the {@link InstallSnapshotResponse}/{@link
     * AcknowledgeEntries} it is asked to send, every {@code setRemoteNodeId} binding
     * (stage 6(k)) and every {@link AnnounceClusterTopology} it is asked to announce
     * (the introduction reply). Construction with a {@code null} id models a connection
     * that is still unbound (nothing has resolved who is on the other side) -- the shape
     * the 6(k) announce pins drive.
     */
    private static final class RecordingSender implements NodeConnectionOutbound {
        private final String remoteNodeId;
        private final List<InstallSnapshotResponse> snapshotResponses = new ArrayList<>();
        private final List<AcknowledgeEntries> acknowledges = new ArrayList<>();
        private final List<String> remoteNodeIdBindings = new ArrayList<>();
        private final List<AnnounceClusterTopology> announcedTopologies = new ArrayList<>();

        RecordingSender(String remoteNodeId) {
            this.remoteNodeId = remoteNodeId;
        }

        @Override
        public String getRemoteNodeId() {
            return remoteNodeId;
        }

        @Override
        public void setRemoteNodeId(String remoteNodeId) {
            remoteNodeIdBindings.add(remoteNodeId);
        }

        @Override
        public NodeAddress getRemoteNodeAddress() {
            return new NodeAddress("stub-" + (remoteNodeId == null ? "unbound" : remoteNodeId));
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
            announcedTopologies.add(announceClusterTopology);
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

        List<String> remoteNodeIdBindings() {
            return List.copyOf(remoteNodeIdBindings);
        }

        List<AnnounceClusterTopology> announcedTopologies() {
            return List.copyOf(announcedTopologies);
        }
    }
}
