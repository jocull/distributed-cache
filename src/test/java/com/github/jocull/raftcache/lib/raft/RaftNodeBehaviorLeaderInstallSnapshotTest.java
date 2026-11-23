package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.data.DataVersion;
import com.github.jocull.raftcache.lib.data.Snapshot;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.raft.middleware.PassThruMiddleware;
import com.github.jocull.raftcache.lib.topology.InMemoryTopologyDiscovery;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 5 (SNAPSHOTS.md): the LEADER side of the snapshot protocol, driven deterministically
 * in-process the way {@link RaftNodeBehaviorPreVoteTest} drives its behaviors — a real node is
 * built, its log seeded past the laggard threshold, and the leader's send/receive paths are
 * invoked directly with {@link RecordingConnection} stubs standing in for the peers. No network,
 * no waiting on the node executor: every assertion observes a synchronous, event-bus-mediated
 * mutation. The 50 ms heartbeat timer the send path schedules is cancelled by closing the
 * behavior at the end of each test, so it can never add a send after the assertions.
 *
 * <ul>
 *   <li><b>Laggard switch</b> — a follower {@code SNAPSHOT_TRIGGER_ENTRIES} or more behind the
 *       committed point gets an {@link InstallSnapshot} instead of appends, exactly once, with
 *       the committed point as its boundary.</li>
 *   <li><b>Back-compat</b> — a cluster with no snapshot supplier (plain raft, no state machine)
 *       keeps sending appends forever, and a supplier that refuses to capture (null payload)
 *       falls back to the slow replay path rather than stalling the follower.</li>
 *   <li><b>Compaction</b> — when every live follower is at the committed point, the log compacts
 *       to the captured snapshot (start point moves, payload recorded); while any follower lags,
 *       it must NOT (S1: the start point never outruns a live follower).</li>
 *   <li><b>Recovery</b> — the install-snapshot response is adopted as the follower's new start
 *       point and appending resumes behind it with no re-sends; a strictly higher term in the
 *       response steps the leader down (Raft term monotonicity).</li>
 * </ul>
 */
class RaftNodeBehaviorLeaderInstallSnapshotTest {
    // The node's entries and its leadership both sit at term 1 (PreVoteTest's
    // leader-of-term-1 setup). Six entries means the laggard at index 1 sits exactly
    // one beyond the trigger threshold, so the boundary math is pinned.
    private static final int TERM = 1;
    private static final long LAST_INDEX = RaftNodeBehaviorLeader.SNAPSHOT_TRIGGER_ENTRIES + 1;
    private static final long LAGGARD_INDEX = 1L;

    /** Convenience for the messages-package TermIndex (clashes with the package-level one). */
    private static com.github.jocull.raftcache.lib.raft.messages.TermIndex ti(int term, long index) {
        return new com.github.jocull.raftcache.lib.raft.messages.TermIndex(term, index);
    }

    private static NodeStates stateOf(RaftNodeImpl node) {
        return ((RaftNodeBehavior) node.currentReceiver()).state;
    }

    private static int termOf(RaftNodeImpl node) {
        return ((RaftNodeBehavior) node.currentReceiver()).term;
    }

    /**
     * The exact serialization the leader performs on its laggard pass (JDK
     * {@code ObjectOutputStream} over the supplier payload): tests compare the wire
     * chunk against this byte stream so they pin the "one serialization, many chunks"
     * property (SNAPSHOTS.md stage 6a) without depending on object identity.
     */
    private static byte[] chunk(Object payload) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(payload);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize test payload", e);
        }
        return out.toByteArray();
    }

    /**
     * A deterministic LEADER of {@link #TERM}: its log already holds entries
     * {@code 1..LAST_INDEX} (so it is past the laggard threshold), it registers two peers so
     * the majority is 2 and it never self-elects, and it is stepped straight to leader. At
     * conversion time it holds no connections yet, so nothing is committed and no heartbeat
     * timer is running — every effect observed below is driven explicitly by the test.
     */
    private static RaftNodeImpl newLeader(RaftManager raftManager) {
        final RaftNodeImpl node = (RaftNodeImpl) raftManager.newNode("node", new NodeAddress("node-addr"));
        for (long i = 1; i <= LAST_INDEX; i++) {
            node.logs.appendLog(TERM, "entry-" + i);
        }
        node.clusterTopology.register(new NodeIdentifier("peerA", new NodeAddress("peerA-addr")));
        node.clusterTopology.register(new NodeIdentifier("peerB", new NodeAddress("peerB-addr")));
        node.start().join();
        node.convertToCandidate(TERM);
        final RaftNodeBehaviorLeader leader = node.convertToLeader();
        assertEquals(NodeStates.LEADER, leader.state);
        assertEquals(TERM, leader.term);
        assertEquals(0L, node.logs.getCommittedTermIndex().getIndex(), "nothing should be committed before the test drives an ack");
        return node;
    }

    private static void closeCurrent(RaftNodeImpl node) {
        // Cancel whatever the current behavior has scheduled (the leader's 50 ms heartbeat,
        // a follower's election timer) so it cannot interleave further sends after we assert.
        ((RaftNodeBehavior) node.currentReceiver()).close();
    }

    // ---- Laggard switch: InstallSnapshot instead of appends ----

    @Test
    void laggard_follower_receivesInstallSnapshot_notAppends() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object payload = new Snapshot(TERM, LAST_INDEX, Map.of());
            node.setSnapshotSupplier(() -> payload);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // The up-to-date follower's ack is what moves the commit point past the laggard:
            // majority is 2 of {upToDate, laggard, leader}, so committed becomes LAST_INDEX
            // with the laggard at LAGGARD_INDEX -- exactly the stage-5 laggard condition.
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));

            final InstallSnapshot install = laggard.installSnapshotsSent.get(0);
            assertEquals(TERM, install.getTerm());
            assertEquals("node", install.getLeaderId());
            assertEquals(LAST_INDEX, install.getLastIncludedIndex(), "the boundary must be the committed point we captured");
            assertEquals(TERM, install.getLastIncludedTerm());
            assertEquals(0L, install.getOffset(), "the transfer starts at byte 0");
            assertTrue(install.isDone(), "below the default 64 KiB chunk size, the whole capture fits one chunk");
            assertTrue(install.getPayload() instanceof byte[], "stage 6a: the wire payload is a serializable byte[] chunk, not the capture object");
            assertArrayEquals(chunk(payload), (byte[]) install.getPayload(),
                    "the chunk IS the leader's single pinned serialization of the capture");

            assertEquals(1, laggard.installSnapshotsSent.size(), "one install, not a burst");
            assertTrue(laggard.appendEntriesSent.isEmpty(), "a laggard gets the snapshot, not appends");
            // upToDate is at our tail, so whatever reaches it (the quiet CONTINUATION, or a
            // later background heartbeat tick) must be an EMPTY append -- never entries.
            assertTrue(upToDate.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "an up-to-date follower only ever receives empty heartbeats");

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex(), "the ack must still commit the log");
            assertFalse(node.logs.hasStartPointSnapshot(), "must NOT compact while a follower lags behind the commit point (S1)");

            closeCurrent(node);
        }
    }

    // ---- Back-compat: no supplier / refusing supplier keeps the plain append path ----

    @Test
    void withoutSnapshotSupplier_plainAppendsAreKept() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            // No snapshot supplier: a plain raft cluster (no state machine) must keep its
            // original behavior -- appends, forever -- even for an arbitrarily far laggard.
            assertEquals(null, node.snapshotSupplier());

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);

            // The laggard gets the full missing range, entries 2..LAST_INDEX in ascending order.
            assertEquals(1, laggard.appendEntriesSent.size());
            final List<AppendEntries.RaftLog> entries = laggard.appendEntriesSent.get(0).getEntries();
            assertEquals(RaftNodeBehaviorLeader.SNAPSHOT_TRIGGER_ENTRIES, entries.size());
            assertEquals(LAGGARD_INDEX + 1, entries.get(0).getTermIndex().getIndex());
            assertEquals(LAST_INDEX, entries.get(entries.size() - 1).getTermIndex().getIndex());
            assertTrue(laggard.installSnapshotsSent.isEmpty(), "no supplier, no installs");

            // The up-to-date follower still receives the (empty) INITIAL append that declares leadership.
            assertEquals(1, upToDate.appendEntriesSent.size());
            assertTrue(upToDate.appendEntriesSent.get(0).getEntries().isEmpty(), "an up-to-date INITIAL send is a heartbeat");

            assertEquals(0L, node.logs.getCommittedTermIndex().getIndex(), "an INITIAL send must not commit anything");
            closeCurrent(node);
        }
    }

    @Test
    void supplierRefusingNullPayload_fallsBackToAppends() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> null); // the state machine refuses to capture

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));

            // Laggard condition held (committed LAST_INDEX, laggard at LAGGARD_INDEX) but the
            // supplier returned null: instead of stalling, the leader replays the missing range.
            assertTrue(laggard.installSnapshotsSent.isEmpty(), "no install when the supplier refuses");
            assertEquals(1, laggard.appendEntriesSent.size(), "the slow replay path must still send");
            assertEquals(RaftNodeBehaviorLeader.SNAPSHOT_TRIGGER_ENTRIES,
                    laggard.appendEntriesSent.get(0).getEntries().size());

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex());
            assertFalse(node.logs.hasStartPointSnapshot(), "no compaction while a follower lags");
            closeCurrent(node);
        }
    }

    @Test
    void gapJustBelowTrigger_staysOnTheAppendPath() {
        // Branch (b) of the laggard decision needs the FULL gap (committed - followerIndex
        // >= SNAPSHOT_TRIGGER_ENTRIES): one entry short of it, the follower is merely
        // behind, not a laggard -- it gets the slow (but correct) append replay, no install.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> new Snapshot(TERM, LAST_INDEX, Map.of()));

            // Laggard at index 2 of 6: once quorum acks the tail, the commit point is 6 and
            // the gap is 4 -- exactly SNAPSHOT_TRIGGER_ENTRIES - 1, short of the trigger.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, 2L));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex());
            assertTrue(laggard.installSnapshotsSent.isEmpty(), "gap < trigger: this follower is behind, not a laggard, so no install");
            assertEquals(1, laggard.appendEntriesSent.size(), "the missing range is still delivered");
            final List<AppendEntries.RaftLog> entries = laggard.appendEntriesSent.get(0).getEntries();
            assertEquals((int) (LAST_INDEX - 2L), entries.size(), "the replay carries exactly the entries the follower lacks");
            assertEquals(3L, entries.get(0).getTermIndex().getIndex(), "the replay starts where the follower's log ends");
            assertEquals(LAST_INDEX, entries.get(entries.size() - 1).getTermIndex().getIndex());
            assertFalse(node.logs.hasStartPointSnapshot(), "S1: no compaction while a live follower lags behind the commit point");
            closeCurrent(node);
        }
    }

    // ---- Compaction: only when every live follower is at the committed point ----

    @Test
    void allFollowersAtCommittedPoint_logCompactsToSnapshot() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object payload = new Snapshot(TERM, LAST_INDEX, Map.of());
            node.setSnapshotSupplier(() -> payload);

            // Every live follower already holds the committed tail, so S1 imposes no bound.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection caughtUp = new RecordingConnection("caughtUp", new TermIndex(TERM, LAST_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(caughtUp);

            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));

            assertTrue(node.logs.hasStartPointSnapshot(), "once quorum is at the commit point, the log compacts");
            assertSame(payload, node.logs.getSnapshotPayload(), "the captured state becomes the portable start point");
            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getStartTermIndex());
            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex());
            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCurrentTermIndex(), "a full install keeps the true tail");

            // upToDate and caughtUp are at our tail, so whatever reaches them -- the quiet
            // CONTINUATION, or a background heartbeat tick racing our assertions -- must be an
            // EMPTY append (a heartbeat), never entries. (A strict isEmpty() here is racy: the
            // send path always re-arms the heartbeat timer, and it can tick between the ack
            // and this line; the safety property we actually care about is "no real entries".)
            assertTrue(upToDate.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "an up-to-date follower only ever receives empty heartbeats");
            assertTrue(caughtUp.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "an up-to-date follower only ever receives empty heartbeats");
            assertEquals(NodeStates.LEADER, stateOf(node));
            closeCurrent(node);
        }
    }

    // ---- Recovery: the install response resumes appending behind the boundary ----

    @Test
    void installSnapshotResponse_adoptsTail_andResumesAppends() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object payload = new Snapshot(TERM, LAST_INDEX, Map.of());
            node.setSnapshotSupplier(() -> payload);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // Phase 1: the commit point moves past the laggard on the other follower's ack,
            // and the laggard receives exactly one install.
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            assertEquals(1, laggard.installSnapshotsSent.size());
            assertEquals(new TermIndex(TERM, LAGGARD_INDEX), laggard.getTermIndex(), "the leader learns the new tail from the response, not the ack");

            // Phase 2: the follower finished installing and reports our boundary as its new tail.
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAST_INDEX));

            assertEquals(new TermIndex(TERM, LAST_INDEX), laggard.getTermIndex(), "the response's tail becomes the follower's new start point (Raft §7)");
            assertEquals(1, laggard.installSnapshotsSent.size(), "an accepted response must not re-send the snapshot");
            // Whatever reaches the laggard now (the zero-entry CONTINUATION, or a background
            // heartbeat tick that re-armed and raced our assertions -- the 50 ms timer is
            // rescheduled at the end of every send pass) must be an EMPTY append, never
            // real entries: a strict isEmpty() here is racy for the same reason as the
            // same-named assertion in allFollowersAtCommittedPoint_logCompactsToSnapshot.
            assertTrue(laggard.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "a follower at our tail only ever receives empty heartbeats");
            // upToDate is at our tail: anything that reaches it (the quiet CONTINUATION, or a
            // background heartbeat tick) must be an EMPTY append -- never entries.
            assertTrue(upToDate.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "an up-to-date follower only ever receives empty heartbeats");

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex());
            assertEquals(NodeStates.LEADER, stateOf(node), "our own term in the response keeps us the leader");
            // The response already put the laggard at the committed point (the install IS the
            // acknowledgement of everything up to it), so the compaction gate -- evaluated on
            // every updateCommittedIndex call -- fires here and the log compacts now.
            assertTrue(node.logs.hasStartPointSnapshot(), "every live follower is at the committed point after the install response, so the log compacts");
            assertSame(payload, node.logs.getSnapshotPayload(), "the captured state becomes the portable start point");
            closeCurrent(node);
        }
    }

    // ---- Term monotonicity: a higher term in the response steps the leader down ----

    @Test
    void higherTermSnapshotResponse_stepsDownToFollower() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> new Snapshot(TERM, LAST_INDEX, Map.of()));

            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(laggard);

            // The follower we just installed reports a HIGHER term: a newer leader exists, so
            // we are the laggard now and must step down -- exactly as AcknowledgeEntries does.
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM + 1, LAST_INDEX));

            assertEquals(NodeStates.FOLLOWER, stateOf(node), "a higher term in the response steps us down");
            assertEquals(TERM + 1, termOf(node), "the term advances to the reported one");
            assertEquals(new TermIndex(TERM, LAGGARD_INDEX), laggard.getTermIndex(), "we step down BEFORE adopting the response's tail");
            assertTrue(laggard.installSnapshotsSent.isEmpty());
            closeCurrent(node);
        }
    }

    // ---- Stage 6e: a reply that arrives with no transfer in flight is treated as a completion ----

    @Test
    void aSnapshotResponseBeforeAnyTransfer_isTreatedAsCompletion_andResendsNothing() {
        // Edge of the completion condition (SNAPSHOTS.md 6e): a reply arrives when no transfer
        // is in flight (snapshotsInFlight is empty -- a duplicated, late, or mis-routed reply).
        // The leader must handle it exactly like a completed transfer: adopt the reported tail,
        // attempt no retransmit (there is no pinned capture to re-send), and leave the log alone.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            // No snapshot supplier is registered: even if the commit point lands on a snapshot
            // gate that fires, the log cannot be trimmed either way -- the log is what we assert.

            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(laggard);

            // No ack, no capture: snapshotsInFlight is still empty when the reply arrives. Must
            // not throw out of the handler and must not fabricate a retransmit of a capture
            // that was never made.
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAST_INDEX));

            assertEquals(new TermIndex(TERM, LAST_INDEX), laggard.getTermIndex(), "a stale reply still adopts the reported tail");
            assertTrue(laggard.installSnapshotsSent.isEmpty(), "a reply with no transfer in flight must NOT fabricate a retransmit");
            assertTrue(laggard.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "a follower at our tail only ever receives empty heartbeats");
            assertEquals(NodeStates.LEADER, stateOf(node), "a same-term reply keeps us the leader");
            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex(),
                    "the install IS the acknowledgement of everything up to the reported tail (Raft \u00a77)");
            assertFalse(node.logs.hasStartPointSnapshot(), "no supplier was registered, so the log is left untouched");
            closeCurrent(node);
        }
    }

    // ---- Chunked transfer (stage 6a): one pinned serialization, byte cursor, idempotent re-sends ----

    @Test
    void chunkedSnapshot_restartsFromByteZero_whenTheFollowerRefuses_thenCompletesOnTheFinalChunk() {
        // The capture's serialized size is unknown in advance (JDK header + map), so split
        // it into two chunks of exactly half a byte: shrink the chunk size to half the
        // serialized length and restore it on exit.
        final Object payload = new Snapshot(TERM, LAST_INDEX,
                Map.of("key1", new DataVersion(Map.of("big", new byte[1536]), 7)));
        final long chunkBytes = halfOfSerialized(payload);
        final long originalChunkBytes = RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES;
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = chunkBytes;

            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object pinned;
            pinned = payload;
            node.setSnapshotSupplier(() -> pinned);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // Phase 1: the commit point moves past the laggard and the FIRST chunk is sent
            // (byte 0, done=false because the capture is half a chunk larger than the slice).
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            final byte[] data = chunk(payload);
            final long end1 = chunkBytes;
            assertEquals(1, laggard.installSnapshotsSent.size());
            InstallSnapshot c1 = laggard.installSnapshotsSent.get(0);
            assertEquals(0L, c1.getOffset(), "chunk 1 starts at byte 0");
            assertEquals(LAST_INDEX, c1.getLastIncludedIndex());
            assertFalse(c1.isDone(), "a capture larger than one chunk does not end with chunk 1");
            assertArrayEquals(Arrays.copyOfRange(data, 0, (int) end1), (byte[]) c1.getPayload());

            // Phase 2: the follower REFUSES (reply cursor 0 = "start over"). The leader
            // retransmits chunk 1 from byte 0 on its next pass over the SAME pinned capture
            // (the in-progress reply even pipelines the resend before we drive a pass -- an
            // exact send count is racy, so we assert on the LAST install delivered). The
            // supplier hands back the SAME object every time, so a byte-for-byte match
            // proves the retransmit is the original serialization, not a new capture.
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAGGARD_INDEX, 0L));
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertTrue(laggard.installSnapshotsSent.size() >= 2, "a 'start over' reply must trigger a retransmit");
            c1 = laggard.installSnapshotsSent.get(laggard.installSnapshotsSent.size() - 1);
            assertEquals(0L, c1.getOffset(), "the retransmit starts at byte 0");
            assertArrayEquals(Arrays.copyOfRange(data, 0, (int) end1), (byte[]) c1.getPayload(),
                    "the retransmitted chunk is the SAME pinned serialization, not a new capture");

            // Phase 3: the follower accepts chunk 1 and reports its cursor (byte end1). Any
            // further send is chunk 2, the LAST one (done=true), resuming at the cursor.
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAGGARD_INDEX, end1));
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            final InstallSnapshot c2 = laggard.installSnapshotsSent.get(laggard.installSnapshotsSent.size() - 1);
            assertEquals(end1, c2.getOffset(), "chunk 2 resumes where the follower's cursor left off");
            assertTrue(c2.isDone(), "the final chunk is done");
            assertArrayEquals(Arrays.copyOfRange(data, (int) end1, data.length), (byte[]) c2.getPayload());

            // Phase 4: the done-reply reports our boundary as the follower's new tail: the
            // transfer completes, its tail is adopted, and the FOLLOWING pass is plain
            // heartbeats -- no more installs of a completed capture.
            final int installsAfterCompletion = laggard.installSnapshotsSent.size();
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAST_INDEX, (long) data.length));
            assertEquals(new TermIndex(TERM, LAST_INDEX), laggard.getTermIndex(), "the installed tail is adopted");
            assertEquals(NodeStates.LEADER, stateOf(node), "a same-term reply keeps us the leader");
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertEquals(installsAfterCompletion, laggard.installSnapshotsSent.size(), "a completed transfer is NOT re-sent");
            assertTrue(laggard.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "post-completion traffic to the laggard is heartbeats only");

            closeCurrent(node);
        } finally {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = originalChunkBytes;
        }
    }

    @Test
    void anOverLengthCursorReply_completesTheTransfer_soTheLeaderNeverSeeksPastTheCapture() {
        // Edge of the completion condition (SNAPSHOTS.md 6e): the follower reports a cursor
        // PAST the last byte of the pinned capture while its index is still BELOW our boundary,
        // so the OFFSET (not the index) must trigger completion. Misclassified as in-progress,
        // the leader would adopt the out-of-bounds cursor as its resume point and the next pass
        // would throw out of appendEntries (Arrays.copyOfRange with a start past the capture).
        final Object payload = new Snapshot(TERM, LAST_INDEX,
                Map.of("key1", new DataVersion(Map.of("big", new byte[1536]), 7)));
        final long chunkBytes = halfOfSerialized(payload);
        final long originalChunkBytes = RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES;
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = chunkBytes;

            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> payload);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // Phase 1: the first half-chunk is in flight (offset 0, not done).
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            final byte[] data = chunk(payload);
            assertEquals(1, laggard.installSnapshotsSent.size());
            assertEquals(0L, laggard.installSnapshotsSent.get(0).getOffset());
            assertFalse(laggard.installSnapshotsSent.get(0).isDone());

            // Phase 2: the reply's cursor runs past the capture's last byte while its index stays
            // below our boundary: it must be treated as COMPLETE -- and must not throw.
            leader.onInstallSnapshotResponse(laggard,
                    new InstallSnapshotResponse(TERM, LAGGARD_INDEX, data.length + 1000L));
            assertEquals(new TermIndex(TERM, LAGGARD_INDEX), laggard.getTermIndex(),
                    "the reported tail is adopted verbatim (the cursor is a hint, the index is what we trust)");
            assertEquals(NodeStates.LEADER, stateOf(node), "a same-term reply keeps us the leader");

            // Phase 3: the transfer is torn down and the laggard is below our tail again, so the
            // next pass is a FRESH install (byte 0, the pinned capture) -- never a "resume" from
            // the out-of-bounds cursor.
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertTrue(laggard.installSnapshotsSent.size() >= 2, "a follower below our tail is installed again");
            final InstallSnapshot again = laggard.installSnapshotsSent.get(laggard.installSnapshotsSent.size() - 1);
            assertEquals(0L, again.getOffset(), "the fresh transfer starts at byte 0, not past the capture");
            assertArrayEquals(Arrays.copyOfRange(data, 0, (int) chunkBytes), (byte[]) again.getPayload(),
                    "the fresh transfer re-sends the same pinned capture, not a new one");
            closeCurrent(node);
        } finally {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = originalChunkBytes;
        }
    }

    private static long halfOfSerialized(Object payload) {
        final long len = chunk(payload).length;
        return Math.max(1L, len / 2);
    }

    /**
     * Clear the pacing gate for one more send pass: the reply stamps the ack time at
     * {@code Instant.now()} (millisecond precision), which may not be strictly AFTER the
     * send stamp, so the gate could still be closed when the test drives the next pass.
     */
    private static void openPacingGate(NodeCommunicationState connection) {
        connection.setLastEntriesSent(Instant.EPOCH);
        connection.setLastEntriesAcknowledged(Instant.EPOCH);
    }

    @Test
    void chunkSizeAnExactDivisor_endsOnTheLastByte_doneWithoutAnEmptyTrailer() {
        // Edge of the chunking loop (SNAPSHOTS.md 6a, end = min(sentOffset + chunk, data.length)):
        // when the serialized capture is an EVEN multiple of two, the second chunk lands EXACTLY
        // on the last byte. done must fire on that final chunk — with a FULL payload slice, a
        // naive "while (sentOffset < length)" loop would emit a spurious third, zero-byte chunk.
        final Object payload = new Snapshot(TERM, LAST_INDEX,
                Map.of("key1", new DataVersion(Map.of("big", new byte[256]), 7)));
        final byte[] data = chunk(payload);
        final long chunkBytes = (data.length + 1) / 2; // chunk 1 full-size; chunk 2 reaches the last byte exactly
        final long originalChunkBytes = RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES;
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = chunkBytes;

            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object pinned = payload;
            node.setSnapshotSupplier(() -> pinned);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // Chunk 1: a FULL chunkBytes slice starting at byte 0, not done (it does not reach the end).
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            assertEquals(1, laggard.installSnapshotsSent.size(), "one chunk per pass");
            final InstallSnapshot c1 = laggard.installSnapshotsSent.get(0);
            assertEquals(0L, c1.getOffset());
            assertFalse(c1.isDone());
            assertArrayEquals(Arrays.copyOfRange(data, 0, (int) chunkBytes), (byte[]) c1.getPayload());

            // Chunk 2: the reply cursor is chunkBytes, so the next send resumes there and ends
            // EXACTLY on the last byte: done=true on the chunk that carries the trailing slice.
            // (An in-progress reply may pipeline a resend before we drive a pass, so the count
            // is not exact here -- the LAST delivered install carries the properties.)
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAGGARD_INDEX, chunkBytes));
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            final InstallSnapshot c2 = laggard.installSnapshotsSent.get(laggard.installSnapshotsSent.size() - 1);
            assertEquals(chunkBytes, c2.getOffset(), "the final chunk resumes at the follower's cursor");
            assertTrue(c2.isDone(), "the chunk that reaches the last byte carries the done flag");
            assertArrayEquals(Arrays.copyOfRange(data, (int) chunkBytes, data.length), (byte[]) c2.getPayload(),
                    "the done chunk is a real slice of the capture, not an empty terminator");

            // No empty trailer: nothing in the transfer so far is zero-byte, and once the
            // done-reply completes the transfer, no further pass may emit another chunk at all.
            assertTrue(laggard.installSnapshotsSent.stream().allMatch(i -> ((byte[]) i.getPayload()).length > 0),
                    "a capture of exactly two chunks carries no zero-byte chunk");
            final int installsBeforeCompletion = laggard.installSnapshotsSent.size();
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAST_INDEX, (long) data.length));
            assertEquals(new TermIndex(TERM, LAST_INDEX), laggard.getTermIndex(), "the installed boundary is adopted as the follower's new tail");
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertEquals(installsBeforeCompletion, laggard.installSnapshotsSent.size(), "a completed transfer emits NO further chunk, empty or otherwise");
            assertTrue(laggard.appendEntriesSent.stream().allMatch(a -> a.getEntries().isEmpty()),
                    "post-completion traffic to the laggard is heartbeats only");

            closeCurrent(node);
        } finally {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = originalChunkBytes;
        }
    }

    // ---- SNAPSHOT STORE (stage 6b): compaction persists the wire bytes before the trim ----

    @Test
    void allFollowersAtCommittedPoint_snapshotIsPersistedBeforeCompact() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object payload = new Snapshot(TERM, LAST_INDEX, Map.of());
            node.setSnapshotSupplier(() -> payload);
            final InMemorySnapshotStore store = new InMemorySnapshotStore();
            node.setSnapshotStore(store);

            // Same gate-satisfaction as the stage-5 test: quorum acks the tail and every live
            // follower already holds it, so the compaction is triggered.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection caughtUp = new RecordingConnection("caughtUp", new TermIndex(TERM, LAST_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(caughtUp);

            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));

            assertTrue(node.logs.hasStartPointSnapshot(), "sanity: the log compacted");
            // THE CORE (stage 6b): the compaction persisted the leader's ONE serialization under the
            // committed boundary, BEFORE trimming the log to it (a failed save would have aborted
            // before the trim, so reaching this point guarantees the bytes are durable).
            final SnapshotStore.Stored stored = store.loadLatest()
                    .orElseThrow(() -> new AssertionError("compaction must persist the snapshot"));
            assertEquals(new TermIndex(TERM, LAST_INDEX), stored.boundary(), "the boundary is the committed point");
            assertArrayEquals(chunk(payload), stored.payload(), "the store holds the leader's one serialization");
            closeCurrent(node);
        }
    }

    @Test
    void compactionWithFailingSnapshotStore_failsClosed_logNotTrimmed_committedStillAdvanced() {
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> new Snapshot(TERM, LAST_INDEX, Map.of()));
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
            node.setSnapshotStore(failing);

            // Same gate-satisfaction as the stage-6b test: quorum at the committed point,
            // every live follower there, supplier set -- compaction is triggered.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection caughtUp = new RecordingConnection("caughtUp", new TermIndex(TERM, LAST_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(caughtUp);

            // THE CORE (6c): the failing save aborts the compaction BEFORE the trim and the
            // error escapes to the caller instead of being swallowed -- the leader fails
            // closed: the commit advanced (happening first) but the log is left fully intact
            // and no snapshot is recorded in it, so the next ack retries the capture.
            assertThrows(IllegalStateException.class,
                    () -> leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX))),
                    "a failed save is a hard error, not a silent skip");

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex(),
                    "the commit advance happens before the compaction gate and is not rolled back");
            assertFalse(node.logs.hasStartPointSnapshot(), "fail-safe: no snapshot is recorded in the log");
            // UN-trimmed: every entry behind the commit point is still addressable. Had the
            // trim run, only the boundary record would remain and this range would be empty.
            assertEquals(LAST_INDEX - LAGGARD_INDEX, node.logs.getLogRange(new TermIndex(TERM, LAGGARD_INDEX), 10).size(),
                    "fail-safe: the log was not trimmed while the snapshot save failed");
            closeCurrent(node);
        }
    }

    @Test
    void snapshotSupplierThatThrows_fallsBackToAppends_thePassSurvives_andAHealthySupplierRecovers() {
        // (6d) capture side of the laggard path: snapshotSupplier.get() and the serialization
        // that follows it can THROW (a flaky state machine), not merely return null. This pass
        // iterates every live connection inside one lambda, so an uncaught exception would
        // leave the connections queued AFTER this laggard unserved for this tick, and on
        // reply-driven passes it would escape the message handler. A thrown capture must fail
        // exactly the way a declined one does (the null-payload contract): this follower drops
        // to the appends, the remaining connections are served, no transfer lingers, and the
        // very next pass retries the supplier cleanly.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> {
                throw new IllegalStateException("simulated state-machine capture failure");
            });

            // Order carries the "the pass survives" assertion: needsMore (index LAST_INDEX-1,
            // just below the trigger) sits AFTER the laggard, so an abandoned pass would leave
            // it unserved. On a live pass it receives the appends for the entries past index 5.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            final RecordingConnection needsMore = new RecordingConnection("needsMore", new TermIndex(TERM, LAST_INDEX - 1));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);
            node.activeConnections.add(needsMore);

            // THE CORE (6d): a throwing capture degrades to the appends path WITHOUT escaping
            // the ack handler and WITHOUT abandoning the rest of the connection pass.
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            assertEquals(0, laggard.installSnapshotsSent.size(), "a failed capture must not start a snapshot transfer");
            assertTrue(laggard.appendEntriesSent.stream().anyMatch(a -> a.getEntries().size() >= 1),
                    "the failed follower falls back to the slow appends path");
            assertTrue(needsMore.appendEntriesSent.stream().anyMatch(a -> a.getEntries().size() >= 1),
                    "the pass is not abandoned after the failed capture: the next connection still gets served");
            assertEquals(NodeStates.LEADER, stateOf(node), "a failed capture must not disturb leadership");

            // RECOVERY: a healthy supplier turns the very next pass into a clean transfer —
            // nothing lingers from the failed one (the snapshotsInFlight map was never touched).
            final Object payload = new Snapshot(TERM, LAST_INDEX, Map.of());
            node.setSnapshotSupplier(() -> payload);
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertEquals(1, laggard.installSnapshotsSent.size(), "a healthy supplier resumes the snapshot transfer on the next pass");
            assertEquals(0L, laggard.installSnapshotsSent.get(0).getOffset(), "the resumed transfer starts at byte 0");
            assertEquals(LAST_INDEX, laggard.installSnapshotsSent.get(0).getLastIncludedIndex(), "the boundary is the committed point");
            closeCurrent(node);
        }
    }

    @Test
    void snapshotPayloadThatCantBeSerialized_fallsBackToAppends_withoutEscaping() {
        // (6d) sibling of the previous test, aimed at the SERIALIZATION step: a supplier that
        // hands out a payload that is NOT Serializable blows up inside
        // serializeSnapshotPayload (NotSerializableException) -- the capture never reaches the
        // "transfer started" state. Same contract as a null or throwing supplier: the follower
        // falls back to appends, the pass continues, the leader keeps its position.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            // Deliberately a bare Object: not Serializable, so the leader's exact JDK
            // serialization of it MUST fail (sanity-check the precondition the same way the
            // leader serializes -- a raw ObjectOutputStream, since chunk() swallows the exact
            // exception type into an IllegalStateException).
            final Object payload = new Object() {
            };
            assertThrows(NotSerializableException.class, () -> {
                final ByteArrayOutputStream probe = new ByteArrayOutputStream();
                try (final ObjectOutputStream oos = new ObjectOutputStream(probe)) {
                    oos.writeObject(payload);
                    oos.flush();
                }
            }, "a bare Object must fail JDK serialization the way the leader serializes");

            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> payload);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            final RecordingConnection needsMore = new RecordingConnection("needsMore", new TermIndex(TERM, LAST_INDEX - 1));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);
            node.activeConnections.add(needsMore);

            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            assertEquals(0, laggard.installSnapshotsSent.size(), "a non-serializable payload must not start a snapshot transfer");
            assertTrue(laggard.appendEntriesSent.stream().anyMatch(a -> a.getEntries().size() >= 1),
                    "the failed follower still falls back to the slow appends path");
            assertTrue(needsMore.appendEntriesSent.stream().anyMatch(a -> a.getEntries().size() >= 1),
                    "the pass is not abandoned after the failed capture");
            assertEquals(NodeStates.LEADER, stateOf(node), "a failed capture must not disturb leadership");
            closeCurrent(node);
        }
    }

    @Test
    void aNegativeCursorReply_isTreatedAsStartOverFromByteZero() {
        // (6d) Edge of the reply handling: the cursor on the wire is contractually a byte
        // offset (0 = "start over"), so a NEGATIVE value can only arrive from a misbehaving
        // peer. Adopting one verbatim would wedge the transfer: the leader stores it as the
        // in-progress sentinel and the next chunking pass throws out of Arrays.copyOfRange
        // (end = min(negative + chunk, length), from > to). The only sane reading of a
        // negative cursor is the closest legal one -- 0, restart from byte 0 of the SAME
        // pinned capture.
        final Object payload = new Snapshot(TERM, LAST_INDEX,
                Map.of("key1", new DataVersion(Map.of("big", new byte[1536]), 7)));
        final long chunkBytes = halfOfSerialized(payload);
        final long originalChunkBytes = RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES;
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = chunkBytes;
            final byte[] data = chunk(payload);

            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            final Object pinned = payload;
            node.setSnapshotSupplier(() -> pinned);

            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection laggard = new RecordingConnection("laggard", new TermIndex(TERM, LAGGARD_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(laggard);

            // Chunk 1 is out (byte 0, not done) and the transfer is in progress.
            leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX)));
            assertEquals(1, laggard.installSnapshotsSent.size());
            assertEquals(0L, laggard.installSnapshotsSent.get(0).getOffset());
            assertFalse(laggard.installSnapshotsSent.get(0).isDone());

            // THE CORE (6d): an illegal negative cursor reply is treated as "start over from
            // byte 0" -- no exception, and the retransmit picks up again at byte 0 of the same
            // pinned capture (the reply's index is the laggard's unchanged tail, below the
            // boundary, so completion is out of the question).
            leader.onInstallSnapshotResponse(laggard, new InstallSnapshotResponse(TERM, LAGGARD_INDEX, -1L));
            openPacingGate(laggard);
            leader.appendEntries(RaftNodeBehaviorLeader.AppendEntriesTrigger.INITIAL);
            assertTrue(laggard.installSnapshotsSent.size() >= 2, "a start-over reply must trigger a retransmit");
            final InstallSnapshot resend = laggard.installSnapshotsSent.get(laggard.installSnapshotsSent.size() - 1);
            assertEquals(0L, resend.getOffset(), "a negative cursor is clamped to the restart cursor 0");
            assertArrayEquals(Arrays.copyOfRange(data, 0, (int) chunkBytes), (byte[]) resend.getPayload(),
                    "the retransmit is byte 0 of the SAME pinned capture, not a new one");
            assertEquals(NodeStates.LEADER, stateOf(node), "an illegal reply must not disturb leadership");
            closeCurrent(node);
        } finally {
            RaftNodeBehaviorLeader.SNAPSHOT_CHUNK_BYTES = originalChunkBytes;
        }
    }

    @Test
    void compactionWithThrowingSnapshotSupplier_failsClosed_logNotTrimmed_committedStillAdvanced() {
        // (6c-family, supplier half) the compaction gate calls snapshotSupplier.get() directly.
        // A capture that THROWs (as opposed to failing at the store) is pinned to the SAME
        // fail-closed contract as the failing-store case (compactionWithFailingSnapshotStore_...):
        // the error escapes to the caller unswallowed, the commit advance has already happened
        // (it precedes the gate) and is not rolled back, the log is left fully intact with no
        // snapshot recorded, so the next ack retries the capture -- and nothing reached the
        // store, because the failure is upstream of it.
        try (final PassThruMiddleware middleware = new PassThruMiddleware();
             final RaftManager raftManager = new RaftManager(new InMemoryTopologyDiscovery(), middleware)) {
            final RaftNodeImpl node = newLeader(raftManager);
            final RaftNodeBehaviorLeader leader = (RaftNodeBehaviorLeader) node.currentReceiver();
            node.setSnapshotSupplier(() -> {
                throw new IllegalStateException("simulated state-machine capture failure");
            });
            final InMemorySnapshotStore store = new InMemorySnapshotStore();
            node.setSnapshotStore(store);

            // Same gate satisfaction as the stage-6b tests: quorum at the committed point,
            // every live follower there, supplier set -- compaction is triggered.
            final RecordingConnection upToDate = new RecordingConnection("upToDate", new TermIndex(TERM, LAST_INDEX));
            final RecordingConnection caughtUp = new RecordingConnection("caughtUp", new TermIndex(TERM, LAST_INDEX));
            node.activeConnections.add(upToDate);
            node.activeConnections.add(caughtUp);

            assertThrows(IllegalStateException.class,
                    () -> leader.onAcknowledgeEntries(upToDate, new AcknowledgeEntries(TERM, true, ti(TERM, LAST_INDEX))),
                    "a failed capture is a hard error, not a silent skip");

            assertEquals(new TermIndex(TERM, LAST_INDEX), node.logs.getCommittedTermIndex(),
                    "the commit advance happens before the compaction gate and is not rolled back");
            assertFalse(node.logs.hasStartPointSnapshot(), "fail-safe: no snapshot is recorded in the log");
            assertEquals(LAST_INDEX - LAGGARD_INDEX, node.logs.getLogRange(new TermIndex(TERM, LAGGARD_INDEX), 10).size(),
                    "fail-safe: the log was not trimmed while the supplier failed");
            assertTrue(store.loadLatest().isEmpty(),
                    "the supplier failed before the store was ever consulted");
            closeCurrent(node);
        }
    }

    /**
     * A minimal mutable {@link NodeConnectionOutbound}: the leader's send path mutates its
     * term-index and pacing stamps, so (unlike the read-only pre-vote stubs) the accessors
     * keep real state, defaulting to the fresh-follower values (epoch index, epoch pacing)
     * exactly as a live connection does after the introduction handshake.
     * {@link #sendInstallSnapshot} and {@link #sendAppendEntries} are recorded; everything
     * else is inert.
     */
    private static final class RecordingConnection implements NodeConnectionOutbound {
        private final String remoteNodeId;
        private final List<AppendEntries> appendEntriesSent = new ArrayList<>();
        private final List<InstallSnapshot> installSnapshotsSent = new ArrayList<>();
        private TermIndex termIndex = new TermIndex(0, 0L);
        private Instant lastEntriesSent = Instant.EPOCH;
        private Instant lastEntriesAcknowledged = Instant.EPOCH;

        RecordingConnection(String remoteNodeId) {
            this.remoteNodeId = remoteNodeId;
        }

        RecordingConnection(String remoteNodeId, TermIndex termIndex) {
            this(remoteNodeId);
            this.termIndex = termIndex;
        }

        // ---- NodeCommunicationState (real, since the leader mutates it) ----

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
            return termIndex;
        }

        @Override
        public void setTermIndex(TermIndex termIndex) {
            this.termIndex = termIndex;
        }

        @Override
        public Instant getLastEntriesSent() {
            return lastEntriesSent;
        }

        @Override
        public void setLastEntriesSent(Instant lastEntriesSent) {
            this.lastEntriesSent = lastEntriesSent;
        }

        @Override
        public Instant getLastEntriesAcknowledged() {
            return lastEntriesAcknowledged;
        }

        @Override
        public void setLastEntriesAcknowledged(Instant lastEntriesAcknowledged) {
            this.lastEntriesAcknowledged = lastEntriesAcknowledged;
        }

        // ---- NodeCommunicationSender (record the two we are here for) ----

        @Override
        public void sendAppendEntries(AppendEntries appendEntries) {
            appendEntriesSent.add(appendEntries);
        }

        @Override
        public void sendInstallSnapshot(InstallSnapshot installSnapshot) {
            installSnapshotsSent.add(installSnapshot);
        }

        @Override
        public CompletableFuture<StateResponse> sendStateRequest(StateRequest stateRequest) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendStateResponse(StateResponse stateResponse) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendIntroduction(Introduction introduction) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendVoteRequest(VoteRequest voteRequest) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendVoteResponse(VoteResponse voteResponse) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendPreVoteRequest(PreVoteRequest preVoteRequest) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendPreVoteResponse(PreVoteResponse preVoteResponse) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
            throw new UnsupportedOperationException("test stub");
        }

        @Override
        public void sendInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse) {
            throw new UnsupportedOperationException("test stub");
        }
    }
}
