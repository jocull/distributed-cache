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
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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
