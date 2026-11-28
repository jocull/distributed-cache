package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.events.SnapshotInstalled;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

class RaftNodeBehaviorFollower extends RaftNodeBehavior {
    private CompletableFuture<?> electionTimeout;
    private String votedForNodeId;
    private String leaderId;

    // Pre-vote round (etcd "pre-vote"): before stepping up to candidate, this follower
    // hypothetically asks the cluster for its votes. The round always runs FOR TERM + 1
    // (each behavior instance has a fixed `term`, so a round cannot straddle terms).
    // `preVoteGrants` holds the peer ids that granted; the candidate's own vote is implicit
    // (counted as self, exactly as the candidate counts its own vote).
    private final Set<String> preVoteGrants = new HashSet<>();
    private boolean preVoteRoundActive = false;

    // In-progress snapshot transfer from our leader (SNAPSHOTS.md stage 6a). The leader
    // streams ONE pinned capture in byte-order chunks: the boundary (the
    // last-included term/index pair) is constant across ALL chunks of the transfer,
    // and offset/expectedOffset are pure byte cursors, so a single in-progress slot
    // can never confuse one transfer with another. `expectedOffset` is the next byte
    // the leader must send; on every reply we report it back, which lets the leader
    // resume exactly there after any loss or duplicate. Cleared when the final chunk
    // is installed (and abandoned if a conflicting transfer starts).
    private InProgressSnapshot transferInProgress;

    private static final class InProgressSnapshot {
        final TermIndex boundary;
        final ByteArrayOutputStream buffer;
        long expectedOffset;

        InProgressSnapshot(TermIndex boundary) {
            this.boundary = boundary;
            this.buffer = new ByteArrayOutputStream();
        }
    }

    public RaftNodeBehaviorFollower(RaftNodeImpl self, int term) {
        super(self, NodeStates.FOLLOWER, term);
        scheduleNextElectionTimeout();
    }

    @Override
    void closeInternal() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
            electionTimeout = null;
        }
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    private void scheduleNextElectionTimeout() {
        if (electionTimeout != null
                && !electionTimeout.isCancelled()
                && !electionTimeout.isDone()) {
            electionTimeout.cancel(false);
        }

        // The election timeout is randomized to be between 150ms and 300ms.
        final int timeoutMs = 150 + self.random.nextInt(151);
        final Executor executor = CompletableFuture.delayedExecutor(timeoutMs, TimeUnit.MILLISECONDS, self.nodeExecutor);
        electionTimeout = CompletableFuture.runAsync(this::onElectionTimeout, executor);
        LOGGER.trace("{} Scheduled next election timeout ({} ms)", self.getId(), timeoutMs);
    }

    /* package-private-for-test — also invoked asynchronously by the election scheduler */
    void onElectionTimeout() {
        if (isTerminated()) {
            return;
        }
        // Start (or re-start) a pre-vote round for term + 1. Grants start empty; our own
        // pre-vote is implicit and counted separately. Re-use the same election timer to
        // keep the round alive: if no majority of grants arrives before it fires, retry.
        // (etcd "pre-vote": a term is only incremented when we actually become candidate.)
        preVoteRoundActive = true;
        preVoteGrants.clear();

        // Our own vote can constitute a majority only in a one-node cluster, where no peer
        // will ever respond. Take the shortcut directly to candidate (mirrors the
        // candidate's own self-majority shortcut).
        if (1 >= self.clusterTopology.getMajorityCount()) {
            LOGGER.info("{} Own pre-vote is a majority ({} of {}) - advancing to candidate",
                    self.getId(), 1, self.clusterTopology.getMajorityCount());
            self.convertToCandidate(term + 1);
            return;
        }

        // Per Raft §5.4.1 report the term/index of our LAST log entry — identical to the
        // VoteRequest a real candidate for term + 1 would send.
        final TermIndex lastLogTermIndex = self.logs.getCurrentTermIndex();
        final PreVoteRequest preVoteRequest = new PreVoteRequest(term + 1, self.getId(),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        lastLogTermIndex.getTerm(),
                        lastLogTermIndex.getIndex()));
        LOGGER.info("{} Starting a pre-vote round for term {} (own log {})", self.getId(), term + 1, lastLogTermIndex);
        self.getActiveConnections().forEach(c -> c.sendPreVoteRequest(preVoteRequest));

        scheduleNextElectionTimeout();
    }

    @Override
    public void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest) {
        // Stage 6(l) (SNAPSHOTS.md stage 6(l)): the sender's identity is bound by
        // onIntroduction BEFORE any vote traffic can arrive, and the transport layer throws on
        // an unbound vote carrier — so a null remoteNodeId at this point is malformed by
        // construction. Guarding here is defense-in-depth for the direct-handler surface:
        // granting below would record votedForNodeId = null, which the same-term guard above
        // (votedForNodeId != null) would then silently bypass, handing out a SECOND vote in the
        // same term (Raft §5.2 one-vote-per-term). Refuse before any term comparison or
        // conversion — warn + zero state change + no reply, the identity-family shape.
        if (sender.getRemoteNodeId() == null) {
            LOGGER.warn("{} VoteRequest from {} is unbound (null remoteNodeId) - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }
        if (voteRequest.getTerm() < term) {
            LOGGER.warn("{} Received a vote request from {} for a term lower than current term: {} vs {}", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), term);
            return;
        }
        if (voteRequest.getTerm() == term && votedForNodeId != null) {
            LOGGER.warn("{} Received a vote request from {} for term {} but already voted for {}", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), votedForNodeId);
            return;
        }
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and will convert to follower (from term {})", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), term);
            // Voting advances the term but must NOT roll back the log: the up-to-date rule
            // (Raft §5.4.1) compares the candidate's last entry against our CURRENT log,
            // unmutated. See RaftNodeImpl.convertToFollowerForVote.
            self.convertToFollowerForVote(voteRequest.getTerm(), f -> {
                f.onVoteRequest(sender, voteRequest);
                return null;
            });
            return;
        }

        // Stage 6(i) (SNAPSHOTS.md stage 6(i)): a hostile or missing wire tail (null field, or a
        // negative term/index axis) would NPE out of the wire getters or IAE out of the domain
        // build below (the stage 6(g) root mechanism), mid-handler AFTER leaderId had been
        // cleared. Refuse before any of that — the same shape as the ordinary denial below:
        // one explicit false VoteResponse carrying our own committed tail, and a re-armed
        // election timer. Term recognition already happened (the conversion above advanced
        // us), so this refusal is not a step-down, and votedForNodeId stays untouched: the
        // denial must not burn the node's single per-term vote (only a grant below sets it).
        if (isHostileWireTermIndex(voteRequest.getLastLogTermIndex())) {
            LOGGER.warn("{} Received vote request from {} for term {} carrying a hostile or missing last-log term/index ({}) - refusing",
                    self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), voteRequest.getLastLogTermIndex());
            scheduleNextElectionTimeout();
            final TermIndex committedTail = self.logs.getCommittedTermIndex();
            sender.sendVoteResponse(new VoteResponse(
                    voteRequest.getTerm(),
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedTail.getTerm(), committedTail.getIndex()),
                    false));
            return;
        }

        // If the receiving node hasn't voted yet in this term then it votes for the candidate...
        // ...and the node resets its election timeout. Per Raft §5.4.2 (up-to-date rule) the
        // vote is granted only if the candidate's last log entry is at least as up-to-date as
        // the voter's own: higher or equal term, and on equal term, higher or equal index.
        if (leaderId != null) {
            LOGGER.info("{} Removing current leader {}", self.getId(), leaderId);
            leaderId = null; // Remove the current leader
        }

        final TermIndex currentIndex = self.logs.getCurrentTermIndex();
        final TermIndex committedIndex = self.logs.getCommittedTermIndex();
        final TermIndex incomingTermIndex = new TermIndex(
                voteRequest.getLastLogTermIndex().getTerm(), voteRequest.getLastLogTermIndex().getIndex());
        final boolean grantVote = incomingTermIndex.compareTo(currentIndex) >= 0;
        // Grant at most one vote per term, but record votedForNodeId ONLY on a grant (Raft §5.4.1).
        // A denial must not burn the node's single vote: leaving votedForNodeId null lets a more
        // up-to-date candidate that arrives later in the same term still be granted. The old "vote
        // for self instead" behaviour made a node that saw a stale candidate first also refuse the
        // freshest candidate, which could cost an entire empty term to elect the freshest node.
        // votedForNodeId is guaranteed null here — the guard above returns when it is non-null in
        // the same term, so a node that already granted its vote cannot reach this point.
        if (grantVote) {
            votedForNodeId = sender.getRemoteNodeId();
        }
        LOGGER.info("{} vote term {} for {}: grant={} (candidate {} vs own log {})", self.getId(), term,
                sender.getRemoteNodeId(), grantVote, voteRequest.getLastLogTermIndex(), currentIndex);
        // Voting resets the election timeout to let the voting process settle
        scheduleNextElectionTimeout();

        final VoteResponse response = new VoteResponse(
                voteRequest.getTerm(),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedIndex.getTerm(), committedIndex.getIndex()),
                grantVote);

        sender.sendVoteResponse(response);
    }

    @Override
    public void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse) {
        // Stage 6(l) (SNAPSHOTS.md stage 6(l)): unbound sender is malformed by construction
        // (the transport layer throws on the live path); guarding is defense-in-depth for the
        // direct-handler surface. Refuse BEFORE the higher-term branch so the null sender does
        // not even advance our term via convertToFollower. Identity-family shape: warn, zero
        // state change, no reply.
        if (sender.getRemoteNodeId() == null) {
            LOGGER.warn("{} VoteResponse from {} is unbound (null remoteNodeId) - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm(), Function.identity());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but will stay follower of term {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    public void onPreVoteRequest(NodeConnectionOutbound sender, PreVoteRequest preVoteRequest) {
        // A pre-vote request is a HYPOTHETICAL vote: it must leave us fully untouched —
        // no term update, no votedFor mutation, no election-timer reset. We answer only
        // whether we WOULD grant: accept the term (a follower accepts >=, since a real
        // VoteRequest here would advance to it or stay at it) and pass the up-to-date rule
        // against our current, unmutated log (Raft §5.4.2).
        //
        // votedFor is deliberately NOT consulted: a real same-term voter that already granted
        // its vote WOULD deny us here (the one-vote-per-term rule). Not consulting it means a
        // node that already voted for someone else may over-predict a grant — a
        // hypothetical-grant that costs us nothing and never changes state. The over-prediction
        // is liveness-only (it can at most let a stale node start a real election that it will
        // then lose at the vote gate); the real election below remains strictly vote-gated, so
        // safety is unaffected.
        final boolean grant = preVoteRequest.getTerm() >= term && isUpToDate(preVoteRequest);
        LOGGER.info("{} Pre-vote request from {} for term {} vs own term {} - grant={}",
                self.getId(), sender.getRemoteNodeId(), preVoteRequest.getTerm(), term, grant);
        sender.sendPreVoteResponse(new PreVoteResponse(preVoteRequest.getTerm(), grant));
    }

    @Override
    public void onPreVoteResponse(NodeConnectionOutbound sender, PreVoteResponse preVoteResponse) {
        // Stage 6(m) (SNAPSHOTS.md stage 6(m)): unbound sender is malformed by construction
        // (the transport layer throws on the live path); guarding is defense-in-depth for the
        // direct-handler surface. Without this, preVoteGrants.add(null) below is legal for a
        // HashSet and the null grant counts in 1 + preVoteGrants.size(): in a 3-node cluster
        // that is self (+1) + {null} = 2 = the majority, so ONE unattributed grant converts
        // us to candidate (inflation is capped at one by set semantics, but it is exactly
        // majority-reachable in 3 nodes). Refuse before any round bookkeeping.
        // Identity-family shape: warn, zero state change, no reply.
        if (sender.getRemoteNodeId() == null) {
            LOGGER.warn("{} PreVoteResponse from {} is unbound (null remoteNodeId) - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }
        if (!preVoteRoundActive) {
            // A late grant for a round that already concluded (we reached a majority, a
            // leader appeared, or we were converted away). Acting on it now could restart
            // a round that is no longer ours.
            LOGGER.warn("{} Received pre-vote response from {} for term {} while no pre-vote round is active - ignoring",
                    self.getId(), sender.getRemoteNodeId(), preVoteResponse.getTerm());
            return;
        }

        // Each behavior instance has a fixed term, and a round is always held for term + 1,
        // so any response for a different term is stale or foreign — never count it.
        final int roundTerm = term + 1;
        if (preVoteResponse.getTerm() != roundTerm) {
            LOGGER.warn("{} Received pre-vote response from {} for term {} but the active round is for term {} - ignoring",
                    self.getId(), sender.getRemoteNodeId(), preVoteResponse.getTerm(), roundTerm);
            return;
        }
        if (!preVoteResponse.isPreVoteGranted()) {
            // The voter evaluated the up-to-date rule (Raft §5.4.2) against its own log and
            // refused. Counting the denial would let a behind node reach a majority out of
            // no-grants — exactly what pre-vote exists to prevent.
            LOGGER.info("{} Pre-vote from {} for term {} denied - not counting", self.getId(), sender.getRemoteNodeId(), roundTerm);
            return;
        }

        // All callbacks run serialized on this node's single executor, so the set needs no
        // synchronization of its own.
        preVoteGrants.add(sender.getRemoteNodeId());
        final int grantTotal = 1 + preVoteGrants.size();
        LOGGER.info("{} Pre-vote granted by {} for term {} - grants so far {} of {} (including self)",
                self.getId(), sender.getRemoteNodeId(), roundTerm, grantTotal, self.clusterTopology.getClusterCount());
        if (grantTotal >= self.clusterTopology.getMajorityCount()) {
            // A majority of hypothetical grants: safe to increment the term for real.
            preVoteRoundActive = false;
            preVoteGrants.clear();
            self.convertToCandidate(roundTerm);
            LOGGER.info("{} Pre-vote majority reached - converting to candidate for term {}", self.getId(), roundTerm);
        }
    }

    /**
     * Stage 6(h) (SNAPSHOTS.md stage 6(h)): does this append carry a structurally invalid
     * term/index in any of the three wire fields the accept path feeds into
     * {@code new TermIndex(...)} — prev-log index, each entry's index, and the leader
     * commit index? A missing field is hostile too: it would NPE out of the wire getters
     * before the constructor's negative IAE even fires.
     *
     * <p>Stage 6(j) (SNAPSHOTS.md stage 6(j)): a null <i>entry slot</i> in the list is
     * hostile too. The wire list admits them — {@link AppendEntries} copies it without
     * element checks (unlike {@code AnnounceClusterTopology}'s {@code List.copyOf}, whose
     * construction-time NPE blocks them at the boundary) — and the accept path dereferences
     * every slot (each entry's term/index, the last entry's tail, the append loop). Without
     * this check the guard itself would throw while classifying them: refuse, don't throw.
     *
     * <p>Stage 6(n) (SNAPSHOTS.md stage 6(n)): and so is a null <i>payload</i> inside a
     * well-formed entry — the 6(h)/6(j) pins cover hostile term/indexes and null <i>slots</i>,
     * but not a present entry whose {@code entry} field is missing. A wire
     * {@code AppendEntries.RaftLog} is construction-guarded ({@code requireNonNull(entry)}),
     * but Java deserialization reconstructs fields without calling the constructor, so a
     * hostile/corrupted stream can carry a null payload past it. Without this check the accept
     * path hands it to {@code RaftLogs.appendLog}, which tears out any conflicting log entries
     * (real state — the deletion runs first) and only then NPEs out of
     * {@code CompletableRaftLog} construction. A null payload is structurally invalid exactly
     * like a null slot or a missing term/index, so it is refused by this same guard — pre-
     * mutation, false-ack, zero-state-change.
     */
    private static boolean hasHostileWireTermIndex(AppendEntries appendEntries) {
        if (isHostileWireTermIndex(appendEntries.getPreviousLogTermIndex())
                || isHostileWireTermIndex(appendEntries.getLeaderCommitTermIndex())) {
            return true;
        }
        for (AppendEntries.RaftLog entry : appendEntries.getEntries()) {
            if (entry == null || isHostileWireTermIndex(entry.getTermIndex()) || entry.getEntry() == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
        // Stage 6(m) (SNAPSHOTS.md stage 6(m)): an unbound sender is malformed by construction
        // (the transport layer throws on the live path); guarding is defense-in-depth for the
        // direct-handler surface. Without this, a higher-term message steps us down and installs
        // a null leaderId, and an equal-term message walks the accept path to a null-leader
        // append + commit move + true ack to an unauthenticated carrier. The guard also must
        // precede the pre-vote invalidation below: zero state change includes not tearing down
        // OUR OWN in-flight pre-vote round for a message whose sender we cannot even name.
        // Identity-family shape: warn, zero state change, no reply.
        if (sender.getRemoteNodeId() == null) {
            LOGGER.warn("{} AppendEntries from {} is unbound (null remoteNodeId) - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }

        // A live leader just spoke: any in-flight pre-vote round is moot. Invalidate it
        // immediately so late grants cannot resurrect a round and hijack the next
        // election.
        preVoteRoundActive = false;
        preVoteGrants.clear();

        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
            self.convertToFollowerForNewLeader(sender.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(sender, appendEntries);
            return;
        }

        final TermIndex currentTermIndex = self.logs.getCurrentTermIndex();
        final com.github.jocull.raftcache.lib.raft.messages.TermIndex currentTermIndexMessage = new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                currentTermIndex.getTerm(), currentTermIndex.getIndex());
        if (appendEntries.getTerm() < term) {
            LOGGER.warn("{} Received append entries from {} for a term lower than current term: {} vs {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(term, false, currentTermIndexMessage));
            return;
        }

        // (6h) Fail-closed structural guard (SNAPSHOTS.md stage 6h): the accept path below
        // feeds three of this message's wire values -- the prev-log index, every entry's
        // index, and the leader's commit index -- straight into new TermIndex(...), whose
        // constructor throws IAE for a negative term or index (the stage 6(g) root
        // mechanism, this time on the replication path instead of the snapshot path).
        // A missing field is hostile too, NPE-ing out of the wire getters first, and
        // (6j) so is a null entry slot in the list: it would NPE that very dereference.
        // None of those three axes is validity-checked before it is consumed: a hostile
        // or corrupted append carrying one would throw out of the constructor AFTER the
        // leaderId mutation below and, for the entries and the commit index, after some
        // entries are already appended -- the same "throw after mutation" class stage
        // 6(g) closed on the snapshot path. Reject the whole append
        // before any state change, in the exact shape of the sibling refusals above
        // (a false ack answering the message, zero state change; the leader's next
        // heartbeat retries). A clamp cannot be used here the way 6(g)'s tail adoption
        // did: clamping an entry position or the match point would manufacture a bogus
        // append or a phantom match.
        if (hasHostileWireTermIndex(appendEntries)) {
            LOGGER.warn("{} Received append entries from {} carrying an invalid term, index or entry slot ({} @ term {}) - refusing",
                    self.getId(), sender.getRemoteNodeId(), appendEntries, term);
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(term, false, currentTermIndexMessage));
            return;
        }

        final TermIndex incomingTermIndex = new TermIndex(appendEntries.getPreviousLogTermIndex().getTerm(), appendEntries.getPreviousLogTermIndex().getIndex());
        if (!self.logs.containsStartPoint(incomingTermIndex)) {
            // TODO: A chance to get stuck here? What happens if indexes get out of sync?
            //       How should we reset? Will the election timeout take care of it?
            LOGGER.warn("{} Received append entries from {} term {} with invalid index: {} vs {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), appendEntries.getPreviousLogTermIndex(), self.logs.getCurrentTermIndex());
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(term, false, currentTermIndexMessage));
            return;
        }
        if (leaderId != null && !leaderId.equals(sender.getRemoteNodeId())) {
            // TODO: Does this matter...?
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", self.getId(), sender.getRemoteNodeId(), leaderId);
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(term, false, currentTermIndexMessage));
            return;
        }

        // TODO: Move this code to self.convertToFollowerForNewLeader ?
        if (leaderId == null) {
            leaderId = sender.getRemoteNodeId();
            LOGGER.info("{} Made follower of {}", self.getId(), sender.getRemoteNodeId());
        }

        // Append the logs
        if (!appendEntries.getEntries().isEmpty()) {
            final com.github.jocull.raftcache.lib.raft.messages.TermIndex newEndTermIndexMessage = appendEntries.getEntries().get(appendEntries.getEntries().size() - 1).getTermIndex();
            final TermIndex newEndTermIndex = new TermIndex(newEndTermIndexMessage.getTerm(), newEndTermIndexMessage.getIndex());
            LOGGER.debug("{} Received entries from {} for index {} -> {} with {} entries @ term {}, commit {}, {}",
                    self.getId(),
                    sender.getRemoteNodeId(),
                    currentTermIndexMessage,
                    newEndTermIndex,
                    appendEntries.getEntries().size(),
                    appendEntries.getTerm(),
                    self.logs.getCommittedTermIndex(),
                    appendEntries.getLeaderCommitTermIndex());
        } else {
            LOGGER.debug("{} Received heartbeat from {} @ term {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm());
        }

        // TODO: Probably not efficient - pass entire set of new logs and insert them in sequence instead?
        appendEntries.getEntries().forEach(r -> {
            final TermIndex newTermIndex = new TermIndex(r.getTermIndex().getTerm(), r.getTermIndex().getIndex());
            self.logs.appendLog(newTermIndex, r.getEntry());
        });

        // S4: Check for membership entries that add this node - track as pending until catch-up complete
        final boolean hasNewMember = appendEntries.getEntries().stream()
                .map(AppendEntries.RaftLog::getEntry)  // Get the entry object from RaftLog
                .anyMatch(v -> v instanceof com.github.jocull.raftcache.lib.raft.messages.proto.MembershipEntry mem &&
                    mem.getAddMember());  // true = add, false = remove

        if (hasNewMember) {
            self.addPendingJoin(self.getId());
            LOGGER.info("{} New membership entry detected - marked as pending join", self.getId());
        }

        // Align the commit index with the leader
        final TermIndex previousCommitIndex = self.logs.getCommittedTermIndex();
        final TermIndex leaderCommitIndex = new TermIndex(
                appendEntries.getLeaderCommitTermIndex().getTerm(),
                appendEntries.getLeaderCommitTermIndex().getIndex());
        if (leaderCommitIndex.compareTo(previousCommitIndex) > 0) {
            final List<RaftLog<?>> committedLogs = self.logs.commit(leaderCommitIndex);
            // Same apply-then-acknowledge order as the leader path. Follower-replicated
            // entries carry no client futures today, but the ordering stays uniform so the
            // guarantee does not depend on which role a node is in.
            self.eventBus.publish(new LogsCommitted(committedLogs));
            self.logs.notifyCommitted(committedLogs);
            LOGGER.debug("{} Move commit index forward {} -> {} @ term {}", self.getId(), previousCommitIndex, appendEntries.getLeaderCommitTermIndex(), appendEntries.getTerm());

            // S4: Catch-up verification complete for any pending joins - they've now applied all logs up to commit point
            // and can participate in elections/quorum. Remove from pending status if this node was tracked as pending.
            final boolean wasPending = self.isPendingJoin(self.getId());
            if (wasPending) {
                self.removePendingJoin(self.getId());
                LOGGER.info("{} Catch-up verification complete - {} removed from pending joins, eligible to vote", self.getId(), self.getId());
            } else {
                LOGGER.debug("{} Commit advanced but node not pending (already caught up)", self.getId());
            }

            // S4: Check for membership REMOVE operation - if a node is being removed, clear any pending status
            final boolean hasRemoveMember = appendEntries.getEntries().stream()
                    .map(AppendEntries.RaftLog::getEntry)  // Get the entry object from RaftLog
                    .anyMatch(v -> v instanceof com.github.jocull.raftcache.lib.raft.messages.proto.MembershipEntry mem &&
                        !mem.getAddMember());  // false = remove, true = add

            if (hasRemoveMember) {
                LOGGER.debug("{} Found REMOVE membership entry - clearing pending status if any", self.getId());
                self.removePendingJoin(self.getId()); // Clear pending for this node in case it was a newly added member that gets removed
            }
        }

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
        sender.sendAcknowledgeEntries(new AcknowledgeEntries(term, true, currentTermIndexMessage));
    }

    @Override
    public void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm(), Function.identity());
            return;
        }

        LOGGER.warn("{} Received acknowledge entries from {} for term {} but doesn't make sense as follower of term {}", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
    }

    /**
     * Receive a leader's snapshot (SNAPSHOTS.md stage 4, Raft §7). A follower only ever
     * acts on a snapshot from a leader at least as current as itself; otherwise it
     * refuses and reports who it thinks it should be talking to.
     *
     * <p>Order of operations, matching the stage-4 spec:
     * <ol>
     *   <li>Higher term than ours: step down to it (the same "convert to follower of the
     *       new leader" path {@link #onAppendEntries} uses), then re-dispatch this very
     *       message on the fresh follower.</li>
     *   <li>Lower term than ours: refuse and reply with OUR term so the sender can step
     *       down if it still thinks it leads.</li>
     *   <li>Equal term: the sender asserts leadership. Install the snapshot —
     *       <b>adopt into the state machine first, then compact the log to the
     *       boundary</b> — and reply with the new log tail so the leader can resume
     *       {@link AppendEntries} behind the snapshot (stage 5).</li>
     * </ol>
     *
     * <p>The state-machine install is done by publishing a {@link SnapshotInstalled}
     * event on the node's event bus: the {@code StateMachineBridge} (the state-machine
     * owner) is subscribed to that bus and performs the actual {@code adopt}. This keeps
     * the Raft layer from naming any state-machine type — the payload crosses as a loose
     * {@link Object}.
     */
    @Override
    public void onInstallSnapshot(NodeConnectionOutbound sender, InstallSnapshot installSnapshot) {
        // Stage 6(m) (SNAPSHOTS.md stage 6(m)): unbound sender is malformed by construction
        // (the transport layer throws on the live path); guarding is defense-in-depth for the
        // direct-handler surface. Without this, a higher-term snapshot steps us down with a
        // null leader installed, and an equal-term one can reach the leaderId install and
        // the full adopt/compact path (the bigger mutation this surface allows) — both for
        // an unattributed carrier and with no refusal to speak for it. Refuse before any
        // term comparison, conversion, or leaderId write. Identity-family shape: warn,
        // zero state change, no reply.
        if (sender.getRemoteNodeId() == null) {
            LOGGER.warn("{} InstallSnapshot from {} is unbound (null remoteNodeId) - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }
        final int incomingTerm = installSnapshot.getTerm();
        final long lastIncludedIndex = installSnapshot.getLastIncludedIndex();

        // (1) A strictly higher term means this peer learned a leader newer than the one
        // we follow (or we are a laggard from a prior term). Step down first, then
        // re-evaluate this snapshot against our new term.
        if (incomingTerm > term) {
            LOGGER.info("{} Received install-snapshot from {} for term {} above our term {} - converting to follower first",
                    self.getId(), sender.getRemoteNodeId(), incomingTerm, term);
            self.convertToFollowerForNewLeader(sender.getRemoteNodeId(), incomingTerm)
                    .onInstallSnapshot(sender, installSnapshot);
            return;
        }

        // (2) A lower term is stale: we already know a newer term. Refuse and report ours.
        if (incomingTerm < term) {
            final TermIndex tail = self.logs.getCurrentTermIndex();
            LOGGER.warn("{} Received install-snapshot from {} for term {} below our term {} - refusing",
                    self.getId(), sender.getRemoteNodeId(), incomingTerm, term);
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            return;
        }

        // (3) Equal term: the sender claims to lead this term. S2 (progress-only) first:
        // a snapshot is only accepted if it is STRICTLY beyond our current log tail
        // (lastIncludedIndex > tail index). A snapshot at or behind the tail would replace
        // no newer state (it must never replace newer/committed state with older), and
        // refusing a re-sent, already-installed snapshot is what makes redelivery a no-op.
        // The boundary is constant across all chunks of one transfer, so this check
        // applies to every chunk exactly the same way.
        final TermIndex tail = self.logs.getCurrentTermIndex();
        if (lastIncludedIndex <= tail.getIndex()) {
            LOGGER.warn("{} Refusing install-snapshot from {}: boundary index {} is not beyond our tail {} (S2 progress-only)",
                    self.getId(), sender.getRemoteNodeId(), lastIncludedIndex, tail.getIndex());
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            return;
        }

        // (6g) The accept path below builds new TermIndex(lastIncludedTerm, lastIncludedIndex)
        // straight from wire values. The index axis is already gated by S2 (strictly beyond our
        // tail, so >= 0); the term axis is NOT: a same-term peer with a negative boundary term
        // would IAE out of the TermIndex constructor -- after the accept path has already
        // recorded that peer as leader (stage 6e left the accept paths mutation-first for the
        // same reason we now guard here). Refuse with the negative-offset contract: offset-0
        // reply, re-armable election, zero state change.
        final int lastIncludedTerm = installSnapshot.getLastIncludedTerm();
        if (lastIncludedTerm < 0) {
            LOGGER.warn("{} Refusing install-snapshot from {}: boundary term {} is negative (a term is >= 0)",
                    self.getId(), sender.getRemoteNodeId(), lastIncludedTerm);
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            scheduleNextElectionTimeout();
            return;
        }

        // On the accept path: record the sender as this term's leader (if we don't yet
        // know one) so subsequent heartbeats from it are accepted -- the same ordering
        // as onAppendEntries (all validity checks before any state change).
        if (leaderId == null) {
            leaderId = sender.getRemoteNodeId();
            LOGGER.info("{} Made follower of {} (on install-snapshot)", self.getId(), leaderId);
        }

        // (4) Chunk cursor (SNAPSHOTS.md stage 6a). The payload arrives as byte[] chunks
        // of one pinned capture: offset is the chunk's byte position, done marks the
        // final chunk. Our reply's offset is always the next expected byte (0 on
        // conflict = "start over"), which is the leader's resume point.
        final TermIndex boundary = new TermIndex(lastIncludedTerm, lastIncludedIndex);
        final long offset = installSnapshot.getOffset();
        if (offset < 0L) {
            // A negative offset is not a byte position: a misbehaving or buggy peer sent an
            // illegal cursor (the leader clamps a negative RESPONSE cursor for the same
            // reason, SNAPSHOTS.md stage 6d). Without this check a negative offset under an
            // in-progress transfer would fall into the "duplicate" branch below and re-report
            // a stale cursor; tear the transfer down instead so the refusal re-baselines it.
            transferInProgress = null;
            LOGGER.warn("{} Refusing install-snapshot from {}: negative offset {} (offsets are byte positions >= 0; the transfer restarts from byte 0)",
                    self.getId(), sender.getRemoteNodeId(), offset);
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            scheduleNextElectionTimeout();
            return;
        }
        final InProgressSnapshot pending;
        if (offset == 0L) {
            // Fresh start (or a legitimate re-send of byte 0): replace any in-progress
            // transfer. Re-sending chunk 0 is the follower's idempotency escape valve.
            pending = new InProgressSnapshot(boundary);
            transferInProgress = pending;
        } else {
            pending = this.transferInProgress;
            if (pending == null || !pending.boundary.equals(boundary)) {
                // No transfer in flight under this boundary (it began with a different
                // capture, or was reset): we cannot place this chunk -- start over.
                LOGGER.warn("{} Refusing snapshot chunk at offset {} from {} (no in-progress transfer at boundary {})",
                        self.getId(), offset, sender.getRemoteNodeId(), boundary);
                sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
                scheduleNextElectionTimeout();
                return;
            }
            if (offset < pending.expectedOffset) {
                // Duplicate/overlap (lost-ack retransmit, or re-send of an already-
                // appended chunk): drop it, and report our cursor so the leader resumes
                // past it instead of re-feeding bytes we already hold.
                LOGGER.debug("{} Dropping duplicate snapshot chunk at offset {} from {} (expected {})",
                        self.getId(), offset, sender.getRemoteNodeId(), pending.expectedOffset);
                sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), pending.expectedOffset));
                scheduleNextElectionTimeout();
                return;
            }
            if (offset > pending.expectedOffset) {
                // Gap: we are missing bytes -- refuse with cursor 0 so the transfer
                // restarts from byte 0 over the same pinned capture.
                LOGGER.warn("{} Refusing snapshot chunk at offset {} from {} (expected {}; transfer restarts from byte 0)",
                        self.getId(), offset, sender.getRemoteNodeId(), pending.expectedOffset);
                sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
                scheduleNextElectionTimeout();
                return;
            }
            // offset == pending.expectedOffset: in order, fall through and append.
        }

        final Object rawPayload = installSnapshot.getPayload();
        if (rawPayload == null) {
            // An explicit null payload is not part of the chunk protocol (a null CAPTURE is
            // handled on the leader side; the wire always carries a byte[] slice). Left
            // unchecked it NPEs below (chunk.length) and escapes out of the handler with no
            // reply at all; refuse exactly like a malformed payload instead.
            transferInProgress = null;
            LOGGER.error("{} Refusing snapshot from {}: payload is null (expected a byte[] chunk)",
                    self.getId(), sender.getRemoteNodeId());
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            scheduleNextElectionTimeout();
            return;
        }
        final byte[] chunk;
        try {
            chunk = (byte[]) rawPayload;
        } catch (ClassCastException e) {
            // Not a chunk payload (a non-byte[] payload is not part of the chunk
            // protocol): the transfer is unusable -- tear it down and refuse.
            transferInProgress = null;
            LOGGER.error("{} Refusing snapshot from {}: payload is {} (expected a byte[] chunk)",
                    self.getId(), sender.getRemoteNodeId(), String.valueOf(installSnapshot.getPayload()).getClass().getName());
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), 0L));
            scheduleNextElectionTimeout();
            return;
        }
        pending.buffer.write(chunk, 0, chunk.length);
        pending.expectedOffset += chunk.length;

        if (!installSnapshot.isDone()) {
            LOGGER.debug("{} Appended snapshot chunk at offset {} from {} ({} bytes; expected now {})",
                    self.getId(), offset, sender.getRemoteNodeId(), chunk.length, pending.expectedOffset);
            // Reply with our cursor (the leader resumes at exactly this byte) -- the
            // transfer is not yet installable, so the term/index we report is our
            // CURRENT tail, unchanged.
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tail.getIndex(), pending.expectedOffset));
            scheduleNextElectionTimeout();
            return;
        }

        // Final chunk: the concatenated buffer is now the complete JDK-serialized
        // capture. Deserialize, then install with the S4 order (adopt state first,
        // compact the log to the boundary second).
        final Object payload;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(pending.buffer.toByteArray()))) {
            payload = ois.readObject();
        } catch (Exception e) {
            transferInProgress = null;
            final TermIndex tailNow = self.logs.getCurrentTermIndex();
            LOGGER.error("{} Failed to deserialize the snapshot assembled from {} (last chunk {} from {}) - refusing the transfer",
                    self.getId(), sender.getRemoteNodeId(), offset, sender.getRemoteNodeId(), e);
            sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, tailNow.getIndex(), 0L));
            scheduleNextElectionTimeout();
            return;
        }
        final long totalBytes = pending.expectedOffset;
        transferInProgress = null;

        // SNAPSHOT STORE (stage 6b): persist the EXACT wire bytes we just assembled (the
        // leader's one serialization, not a re-serialize) before we adopt or compact.
        // Byte-stable and save-before-trim, exactly as the leader path.
        if (self.snapshotStore() != null) {
            self.snapshotStore().save(boundary, pending.buffer.toByteArray());
        }

        // Advance the state machine FIRST (adopt), then compact the log to the boundary
        // (S4 order: state reflects the boundary before the log is trimmed to it).
        self.eventBus.publish(new SnapshotInstalled(lastIncludedTerm, lastIncludedIndex, payload));
        self.logs.compact(boundary, payload);

        final TermIndex newTail = self.logs.getCurrentTermIndex();
        LOGGER.info("{} Installed snapshot from {} (boundary {}, {}) over {} bytes - tail now {} @ term {}",
                self.getId(), sender.getRemoteNodeId(), lastIncludedTerm, lastIncludedIndex, totalBytes, newTail, term);
        // Reply with the new tail (so the leader can resume append-entries past the
        // snapshot boundary, stage 5 recovery) and our final cursor (== total bytes);
        // the leader treats either as "transfer complete".
        sender.sendInstallSnapshotResponse(new InstallSnapshotResponse(term, newTail.getIndex(), totalBytes));
        scheduleNextElectionTimeout();
    }
}
