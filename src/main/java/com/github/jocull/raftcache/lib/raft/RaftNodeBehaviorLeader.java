package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.raft.events.AppendLog;
import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class RaftNodeBehaviorLeader extends RaftNodeBehavior {
    private final int heartbeatTimeoutMillis = 50;

    /**
     * A follower this far (by index) below the committed point is a "laggard" (SNAPSHOTS.md
     * stage 5): resending log entries to it is exactly the slow path the snapshot exists to
     * skip, so it is switched to {@link InstallSnapshot} until acknowledged. Small on
     * purpose -- this is a low-write cache workload, not a bulk-replication log, so a few
     * missed entries already qualify; a single entry can never trigger it while progress
     * is normal.
     */
    static final int SNAPSHOT_TRIGGER_ENTRIES = 5;

    /**
     * Bytes per {@link InstallSnapshot} chunk (SNAPSHOTS.md stage 6a). The snapshot
     * payload is JDK-serialized ONCE — pinned to a single capture — and then streamed
     * to the laggard in slices of this size, with the byte cursor exchanged through
     * the request's {@code offset} and the response's {@code offset}. Large enough
     * that a snapshot normally needs a handful of round-trips, small enough to stay
     * well inside a friendly per-frame size on the wire.
     * <p>Package-visible (and volatile) so same-package unit tests can shrink it to
     * a few bytes to exercise multi-chunk progress, and restore it in a
     * try/finally.
     */
    static volatile long SNAPSHOT_CHUNK_BYTES = 64 * 1024L;

    /**
     * In-flight snapshot transfer per laggard, keyed by remote node id. The capture is
     * pinned to a single {@code snapshotSupplier} acquisition at pin time (see the send
     * path): re-running the supplier on every laggard pass would let a changing
     * state machine re-capture between chunks, making the byte cursor meaningless and
     * re-serializing a potentially large payload for every pacing retry.
     */
    private final Map<String, SnapshotTransfer> snapshotsInFlight = new HashMap<>();

    /**
     * One pinned snapshot transfer (leader side, SNAPSHOTS.md stage 6a).
     * <ul>
     *   <li>{@link #data} — the supplier payload, JDK-serialized exactly once;</li>
     *   <li>{@link #boundaryIndex} / {@link #boundaryTerm} — the snapshot's
     *       last-included log coordinate, constant for every chunk of the transfer;</li>
     *   <li>{@link #sentOffset} — the next byte offset to send; it is only ever
     *       changed BY A FOLLOWER REPLY (the follower's reported next-expected offset
     *       is authoritative), never by us optimistically;</li>
     *   <li>{@link #inFlightEnd} — the end of the chunk most recently handed to the
     *       wire, for which we are still awaiting its offset reply.</li>
     * </ul>
     */
    private static final class SnapshotTransfer {
        final byte[] data;
        final long boundaryIndex;
        final int boundaryTerm;
        long sentOffset;
        long inFlightEnd;

        SnapshotTransfer(byte[] data, long boundaryIndex, int boundaryTerm) {
            this.data = data;
            this.boundaryIndex = boundaryIndex;
            this.boundaryTerm = boundaryTerm;
        }
    }

    private static byte[] serializeSnapshotPayload(Object payload) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(payload);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize snapshot payload " + payload, e);
        }
        return out.toByteArray();
    }

    private CompletableFuture<?> heartbeatTimeout;

    private final EventSubscriber entriesSubscriber;

    // Package-visible (not private) so same-package unit tests can drive the send path
    // deterministically without waiting on the node executor.
    enum AppendEntriesTrigger {
        INITIAL,
        TIMEOUT,
        CONTINUATION,
        NEW_LOG,
        ;
    }

    public RaftNodeBehaviorLeader(RaftNodeImpl self, int term) {
        super(self, NodeStates.LEADER, term);

        entriesSubscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof AppendLog) {
                    onAppendLog((AppendLog) event);
                }
            }
        };
        self.eventBus.subscribe(entriesSubscriber);

        // Send the initial append first thing to declare self as leader
        appendEntries(AppendEntriesTrigger.INITIAL);
    }

    @Override
    void closeInternal() {
        self.eventBus.unsubscribe(entriesSubscriber);
        if (heartbeatTimeout != null) {
            heartbeatTimeout.cancel(false);
            heartbeatTimeout = null;
        }
    }

    private AppendEntries.RaftLog transformLog(RaftLog<?> r) {
        return new AppendEntries.RaftLog(
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(r.getTermIndex().getTerm(), r.getTermIndex().getIndex()),
                r.getEntry());
    }

    private void onAppendLog(AppendLog appendLog) {
        if (isTerminated()) {
            return;
        }
        CompletableFuture.runAsync(() -> appendEntries(AppendEntriesTrigger.NEW_LOG), self.nodeExecutor);
    }

    private void onHeartbeatTimeout() {
        if (isTerminated()) {
            return;
        }

        LOGGER.debug("{} Heartbeat interval fired", self.getId());
        heartbeatTimeout = null; // Erase now to avoid cancelling self on reschedule
        appendEntries(AppendEntriesTrigger.TIMEOUT);
    }

    // Package-visible so same-package unit tests can drive the send path directly
    // (otherwise appendEntries is only reachable through the event bus / executor).
    void appendEntries(AppendEntriesTrigger trigger) {
        final TermIndex committedTermIndex = self.logs.getCommittedTermIndex();
        final com.github.jocull.raftcache.lib.raft.messages.TermIndex committedTermIndexMessage =
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedTermIndex.getTerm(), committedTermIndex.getIndex());

        self.activeConnections.forEach(c -> {
            if (!c.shouldSendNextEntries(heartbeatTimeoutMillis)) {
                LOGGER.debug("{} Still waiting for ack before send entries to {} - last send {}, receive {}", self.id, c.getRemoteNodeId(), c.getLastEntriesSent(), c.getLastEntriesAcknowledged());
                return;
            }

            // Laggard switch (SNAPSHOTS.md stage 5, Raft §7): appends alone can no longer
            // catch this follower up -- install a snapshot instead, then resume appends
            // ahead of its boundary. Only when a snapshot source is available (a plain
            // raft cluster with no state machine keeps the old behavior: appends, forever).
            //
            // Two triggers; either of them sends the install:
            //   (a) BELOW THE START POINT -- the follower's index is strictly below the
            //       log's compacted boundary, so it needs at least one entry the leader
            //       has already dropped. getLogRange never serves records at or below the
            //       seed, so no append sequence can bridge the gap: the snapshot is the
            //       ONLY catch-up path (Raft §7.2 -- the leader "sends the snapshot if
            //       the follower is missing entries"). No threshold applies here. Strictly
            //       below, because the boundary record itself survives and a follower AT
            //       the boundary replays the entries behind it normally.
            //   (b) BEYOND A THRESHOLD GAP -- the committed gap is large enough that
            //       replay is the slow path; the snapshot is the fast path.
            //
            // The send above is gated by the same pacing/ack rule as appends, which makes
            // this a natural RETRANSMIT: if the InstallSnapshot or its reply is lost, the
            // follower's nextIndex never advances, the laggard condition still holds on the
            // next tick, and this branch sends the install again instead of stalling. That
            // is safe because delivery is idempotent -- a follower refuses (or no-ops) a
            // snapshot that makes no progress beyond its tail (S2) and answers with its own
            // term + tail either way (RaftNodeBehaviorFollower#onInstallSnapshot), so the
            // leader's nextIndex converges to the truth on the first reply that lands.
            final Supplier<Object> snapshotSupplier = self.snapshotSupplier();
            final long followerIndex = c.getTermIndex().getIndex();
            final TermIndex startPoint = self.logs.getStartTermIndex();
            if (snapshotSupplier != null
                    && (followerIndex < startPoint.getIndex()
                        || committedTermIndex.getIndex() - followerIndex >= SNAPSHOT_TRIGGER_ENTRIES)) {
                // Pin the capture to ONE byte stream and stream it in chunks (SNAPSHOTS.md
                // stage 6a). The transfer state survives pacing-gated retries: the
                // follower's offset reply is authoritative for where we resume from
                // (onInstallSnapshotResponse), and we never re-run the supplier for a
                // transfer already in flight.
                final String nodeId = c.getRemoteNodeId();
                SnapshotTransfer transfer = snapshotsInFlight.get(nodeId);
                if (transfer == null) {
                    final Object payload = snapshotSupplier.get();
                    if (payload == null) {
                        // The state machine refused to capture (e.g. nothing to capture yet):
                        // fall back to the slow replay path rather than stall this follower.
                        // (For a below-start-point follower that replay path is empty, but a
                        // null capture only ever happens while nothing has been applied, which
                        // is transient as soon as entries exist.)
                        LOGGER.warn("{} Follower {} is {} behind committed {} but the snapshot source returned null - falling back to appends",
                                self.id, nodeId, committedTermIndex.getIndex() - followerIndex, committedTermIndex);
                    } else {
                        transfer = new SnapshotTransfer(
                                serializeSnapshotPayload(payload),
                                committedTermIndex.getIndex(),
                                committedTermIndex.getTerm());
                        snapshotsInFlight.put(nodeId, transfer);
                        LOGGER.info("{} Follower {} is {} below committed {} (start point {}) - transferring {} bytes of snapshot in {}-byte chunks",
                                self.id, nodeId,
                                committedTermIndex.getIndex() - followerIndex, committedTermIndex, startPoint,
                                transfer.data.length, SNAPSHOT_CHUNK_BYTES);
                    }
                }
                if (transfer != null) {
                    final long end = Math.min(transfer.sentOffset + SNAPSHOT_CHUNK_BYTES, transfer.data.length);
                    final boolean done = end >= transfer.data.length;
                    final byte[] chunk = transfer.data.length == 0
                            ? transfer.data
                            : Arrays.copyOfRange(transfer.data, (int) transfer.sentOffset, (int) end);
                    LOGGER.debug("{} Sending snapshot chunk [{}..{}) of {} bytes to {} (done={})",
                            self.id, transfer.sentOffset, end, transfer.data.length, nodeId, done);
                    c.sendInstallSnapshot(new InstallSnapshot(
                            term,
                            self.getId(),
                            transfer.boundaryIndex,
                            transfer.boundaryTerm,
                            transfer.sentOffset,
                            chunk,
                            done));
                    transfer.inFlightEnd = end;
                    c.setLastEntriesSent(Instant.now());
                    return; // inside the forEach lambda: this is the "continue" for this connection
                }
            }

            final List<AppendEntries.RaftLog> entries = self.logs.getLogRange(c.getTermIndex(), 200, this::transformLog);
            if (entries.isEmpty() && trigger == AppendEntriesTrigger.CONTINUATION) {
                // Continuations only send them if there's entries to send, otherwise they stay quiet
                LOGGER.debug("{} No logs remaining to send on continuation to {}", self.getId(), c.getRemoteNodeId());
                return;
            }

            if (!entries.isEmpty()) {
                final com.github.jocull.raftcache.lib.raft.messages.TermIndex newEndTermIndex = entries.get(entries.size() - 1).getTermIndex();
                LOGGER.debug("{} Appending entries to {} setting index {} -> {} with {} entries @ term {}", self.getId(), c.getRemoteNodeId(), c.getTermIndex(), newEndTermIndex, entries.size(), term);
            } else {
                LOGGER.debug("{} Sending heartbeat to {}", self.getId(), c.getRemoteNodeId());
            }

            c.sendAppendEntries(new AppendEntries(
                    term,
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(c.getTermIndex().getTerm(), c.getTermIndex().getIndex()),
                    committedTermIndexMessage,
                    entries));
            c.setLastEntriesSent(Instant.now());
        });

        // A single node has no replicas to sync with: nothing would ever acknowledge the
        // entries to drive updateCommittedIndex. Its entries are already replicated to the
        // whole cluster (itself), so commit them directly. Skip the heartbeat timer as well --
        // with no connections to keep alive the target delay degenerates to an immediate
        // (busy) reschedule.
        if (self.activeConnections.isEmpty()) {
            updateCommittedIndex();
            return;
        }

        // After appending, reset the next heartbeat
        if (heartbeatTimeout != null && !heartbeatTimeout.isDone()) {
            heartbeatTimeout.cancel(false);
        }

        // What should the next timeout be anyway? Find the lowest common denominator.
        // Needs to target the time that is (lastSent + timeout) from now
        final int targetTimeoutMillis = self.activeConnections.stream()
                .map(NodeCommunicationState::getLastEntriesSent)
                .min(Instant::compareTo)
                .map(minTime -> {
                    final Instant targetTime = minTime.plus(heartbeatTimeoutMillis, ChronoUnit.MILLIS);
                    final long targetMillis = Duration.between(Instant.now(), targetTime).toMillis();
                    if (targetMillis < 0) {
                        return 0;
                    }
                    if (targetMillis > heartbeatTimeoutMillis) {
                        return heartbeatTimeoutMillis;
                    }
                    return (int) targetMillis;
                })
                .orElse(0);

        final Executor delayedExecutor = CompletableFuture.delayedExecutor(targetTimeoutMillis, TimeUnit.MILLISECONDS, this.self.nodeExecutor);
        heartbeatTimeout = CompletableFuture.runAsync(this::onHeartbeatTimeout, delayedExecutor);
        LOGGER.trace("{} Scheduled next heartbeat ({} ms)", this.self.getId(), targetTimeoutMillis);
    }

    private void updateCommittedIndex() {
        final List<TermIndex> currentIndices;
        synchronized (self.getActiveConnections()) {
            currentIndices = self.getActiveConnections().stream()
                    .map(NodeConnectionOutbound::getTermIndex)
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        }

        // The leader's own log counts as one of the replicas in the quorum: the majority
        // is n/2+1 over the whole cluster, leader included. Without counting it, a
        // single-node cluster could never commit (nothing to acknowledge), and neither
        // could a two-node cluster, or a three-node cluster left with only the leader
        // and one follower alive.
        currentIndices.add(self.logs.getCurrentTermIndex());
        currentIndices.sort(Comparator.reverseOrder());

        final int majorityCount = self.clusterTopology.getMajorityCount();
        if (currentIndices.size() >= majorityCount) {
            final TermIndex majorityMinimumIndex = currentIndices.get(majorityCount - 1);
            // TODO: Is this safe? Is there a chance that responding entries could exceed the leader's current index/term?
            //       Some kind of term shuffling that would cause chaos?
            if (majorityMinimumIndex.compareTo(self.logs.getCommittedTermIndex()) > 0) {
                LOGGER.debug("{} Has minimum majority index {} to commit", self.getId(), majorityMinimumIndex);
                final List<RaftLog<?>> committedLogs = self.logs.commit(majorityMinimumIndex);
                // Apply-then-acknowledge: publish is synchronous (EventBus#publish), so by the
                // time we acknowledge the clients below, every state machine subscriber has
                // already applied these entries. A client therefore never sees its own write
                // as committed before it has taken effect.
                self.eventBus.publish(new LogsCommitted(committedLogs));
                self.logs.notifyCommitted(committedLogs);
            }
        } else {
            LOGGER.debug("{} Not a majority to commit with {} (need {})", self.getId(), currentIndices, majorityCount);
        }

        // SNAPSHOT: evaluate compaction on EVERY update, not only on the commit-advancing
        // call. The commit point only ever grows, but "every live follower is at or past it"
        // is a property we ACQUIRE over time: a lagging follower converges on the point as
        // it installs the snapshot (install-response path) or acks the replay (ack path),
        // and both are exactly updateCommittedIndex calls. Checking only the commit-advancing
        // call would test the condition the moment the commit passes the laggard's stale
        // index (the check fails, as it must, while the follower lags) and never re-evaluate
        // once the follower converges -- leaving the log uncompacted forever.
        // The invariants are otherwise unchanged: compact only when every live replica
        // already holds the committed point (S1: the new start point must not outrun any
        // live follower), a snapshot is actually available (S4: a pure function of the
        // applied state), and only when there is something behind the start point to drop
        // (compact() itself still re-enforces the forward-only / committed-bound guards, S5).
        if (!self.getActiveConnections().isEmpty() && self.snapshotSupplier() != null
                && self.logs.getCommittedTermIndex().getIndex() > self.logs.getStartTermIndex().getIndex()
                && self.getActiveConnections().stream().allMatch(
                        c -> c.getTermIndex().getIndex() >= self.logs.getCommittedTermIndex().getIndex())) {
            final Object snapshotPayload = self.snapshotSupplier().get();
            if (snapshotPayload != null) {
                LOGGER.info("{} All live followers at committed {} - compacting log to the snapshot boundary",
                        self.getId(), self.logs.getCommittedTermIndex());
                self.logs.compact(self.logs.getCommittedTermIndex(), snapshotPayload);
            } else {
                LOGGER.warn("{} Follower caught up to {} but no snapshot payload available - cannot compact",
                        self.getId(), self.logs.getCommittedTermIndex());
            }
        }
    }

    @Override
    public void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and granted vote as new follower", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm());
            // Voting advances the term but must NOT roll back the log: the up-to-date rule
            // (Raft §5.4.1) compares the candidate's last entry against our CURRENT log,
            // unmutated. See RaftNodeImpl.convertToFollowerForVote.
            self.convertToFollowerForVote(voteRequest.getTerm(), f -> {
                f.onVoteRequest(sender, voteRequest);
                return null;
            });
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as leader of term {}", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), term);
    }

    @Override
    public void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm(), Function.identity());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but already leader of term {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    public void onPreVoteRequest(NodeConnectionOutbound sender, PreVoteRequest preVoteRequest) {
        // A pre-vote request is a hypothetical vote: it must NOT make this leader step
        // down, advance term, or mutate anything. Answer only whether we WOULD grant in a
        // real election for that term: a leader's real vote path grants only on a
        // strictly higher term (same-term requests are refused), plus the up-to-date rule.
        final boolean grant = preVoteRequest.getTerm() > term && isUpToDate(preVoteRequest);
        LOGGER.info("{} Pre-vote request from {} for term {} vs own term {} - grant={}",
                self.getId(), sender.getRemoteNodeId(), preVoteRequest.getTerm(), term, grant);
        sender.sendPreVoteResponse(new PreVoteResponse(preVoteRequest.getTerm(), grant));
    }

    @Override
    public void onPreVoteResponse(NodeConnectionOutbound sender, PreVoteResponse preVoteResponse) {
        // Pre-vote responses are fire-and-forget: per pre-vote semantics they must never
        // advance our term or change our state, even if a higher term is mentioned.
        LOGGER.warn("{} Received pre-vote response from {} for term {} - ignoring (pre-vote responses do not advance term)",
                self.getId(), sender.getRemoteNodeId(), preVoteResponse.getTerm());
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
            self.convertToFollowerForNewLeader(sender.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(sender, appendEntries);
            return;
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as leader of term {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
        final TermIndex currentTermIndex = self.logs.getCurrentTermIndex();
        final AcknowledgeEntries response = new AcknowledgeEntries(term, false, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                currentTermIndex.getTerm(),
                currentTermIndex.getIndex()));

        sender.sendAcknowledgeEntries(response);
    }

    @Override
    public void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm(), Function.identity());
            return;
        }
        if (!acknowledgeEntries.isSuccess()) {
            LOGGER.warn("{} Received AcknowledgeEntries without success from {}: {}, {} @ term {}", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
            final TermIndex previous = sender.getTermIndex();
            sender.setTermIndex(new TermIndex(
                    acknowledgeEntries.getCurrentTermIndex().getTerm(),
                    acknowledgeEntries.getCurrentTermIndex().getIndex()
            ));
            LOGGER.warn("{} Rewound {}: {} -> {}", self.id, sender.getRemoteNodeId(), previous, sender.getTermIndex());
            return;
        }

        // TODO: What happens if we get out-of-order ack's, moving the term/index backwards?
        //       Does that matter? Would it fix itself?
        LOGGER.debug("{} received AcknowledgeEntries from {} moving index {} -> {} @ term {}", self.getId(), sender.getRemoteNodeId(), sender.getTermIndex(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
        sender.setTermIndex(new TermIndex(
                acknowledgeEntries.getCurrentTermIndex().getTerm(),
                acknowledgeEntries.getCurrentTermIndex().getIndex()));

        updateCommittedIndex();
        sender.setLastEntriesAcknowledged(Instant.now());

        // Are there more entries to send? Go ahead and send them!
        appendEntries(AppendEntriesTrigger.CONTINUATION);
    }

    /**
     * A laggard's reply to one of our snapshot chunks (SNAPSHOTS.md stage 6a). The
     * reply's {@code offset} is the follower's byte cursor — its next expected byte —
     * and is AUTHORITATIVE: we adopt it verbatim, never advancing past it and never
     * insisting on our own view.
     * <ul>
     *   <li>cursor at or past the end of the pinned capture, or the follower's reported
     *       index already at/past the capture's boundary — the transfer is complete:
     *       adopt the reported tail, drop the transfer state, and resume appends behind
     *       it (Raft §7 post-snapshot recovery, the CONTINUATION path sends any entries
     *       behind the boundary exactly as the ack path does);</li>
     *   <li>cursor below that (including 0 — the follower's "start over from byte 0"
     *       reply) — rewind {@code sentOffset} there; the next laggard pass
     *       retransmits from that byte over the SAME pinned capture.</li>
     * </ul>
     *
     * <p>Loop guard: the "index already at/past boundary" completion test catches the
     * follower that already holds everything we would resend — a lost done-reply over an
     * already-caught-up follower would otherwise be re-sent forever (refused, replied 0,
     * re-sent ...). Completing is correct in that case: its tail is at or past our
     * boundary, which is precisely the post-install state.
     *
     * <p>No transfer state (stale reply after completion, or a legacy
     * two-argument reply from an older build) lands in the completion branch too:
     * its index IS the follower's tail, the very pre-chunking acceptance semantics.
     *
     * <p>Term handling mirrors {@link #onAcknowledgeEntries}: a strictly higher term in
     * the response means we are the laggard, step down. A lower-or-equal term is the
     * normal reply from the follower we are installing.
     */
    @Override
    public void onInstallSnapshotResponse(NodeConnectionOutbound sender, InstallSnapshotResponse installSnapshotResponse) {
        final int responseTerm = installSnapshotResponse.getTerm();
        final long responseIndex = installSnapshotResponse.getIndex();
        final long responseOffset = installSnapshotResponse.getOffset();

        if (responseTerm > term) {
            LOGGER.info("{} Received install-snapshot response from {} for term {} above our term {} - stepping down",
                    self.getId(), sender.getRemoteNodeId(), responseTerm, term);
            self.convertToFollower(responseTerm, Function.identity());
            return;
        }

        final String nodeId = sender.getRemoteNodeId();
        final SnapshotTransfer transfer = snapshotsInFlight.get(nodeId);

        if (transfer == null
                || responseIndex >= transfer.boundaryIndex
                || responseOffset >= transfer.data.length) {
            // Transfer complete (or a stale/legacy reply with no transfer to track):
            // the follower reports a log tail at or past our capture boundary. Adopt it.
            snapshotsInFlight.remove(nodeId);

            final TermIndex previous = sender.getTermIndex();
            sender.setTermIndex(new TermIndex(responseTerm, responseIndex));
            LOGGER.info("{} Follower {} installed to {} (was {}) - resuming appends",
                    self.getId(), nodeId, responseIndex, previous);

            // The install is itself an acknowledgement of everything up to the boundary:
            // recompute the commit point in case the response's index outruns it, and let
            // the CONTINUATION append path deliver any entries that sit behind the boundary
            // (Raft §7 recovery). Marking the ack time clears the shouldSendNextEntries gate
            // so the continuation actually sends on this same tick (the same ordering the
            // AcknowledgeEntries path uses).
            updateCommittedIndex();
            sender.setLastEntriesAcknowledged(Instant.now());
            appendEntries(AppendEntriesTrigger.CONTINUATION);
            return;
        }

        // In-progress: adopt the follower's cursor verbatim (0 = "start over from
        // byte 0"; a value equal to our inFlightEnd = the chunk we sent was accepted,
        // keep the cursor and advance next pass; below/above = rewind/jump there).
        LOGGER.debug("{} Follower {} reported snapshot cursor {} (in-flight end {}) - resuming transfer from byte {}",
                self.getId(), nodeId, responseOffset, transfer.inFlightEnd, responseOffset);
        transfer.sentOffset = responseOffset;

        // A reply is proof the follower is live and responsive: open the pacing gate
        // so the next chunk (re)send need not wait out the heartbeat window.
        sender.setLastEntriesAcknowledged(Instant.now());
        // Pipeline: send the next chunk now rather than waiting for the next tick.
        appendEntries(AppendEntriesTrigger.CONTINUATION);
    }
}
