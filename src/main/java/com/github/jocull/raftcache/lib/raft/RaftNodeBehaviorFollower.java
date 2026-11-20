package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

class RaftNodeBehaviorFollower extends RaftNodeBehavior {
    private CompletableFuture<?> electionTimeout;
    private String votedForNodeId;
    private String leaderId;

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

    private void onElectionTimeout() {
        if (isTerminated()) {
            return;
        }
        self.convertToCandidate(term + 1);
    }

    @Override
    public void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest) {
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
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm(), Function.identity());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but will stay follower of term {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
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
}
