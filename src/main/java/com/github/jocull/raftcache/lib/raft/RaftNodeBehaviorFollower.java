package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RaftNodeBehaviorFollower extends RaftNodeBehavior {
    private volatile ScheduledFuture<?> electionTimeout;
    private volatile String votedForNodeId;
    protected volatile String leaderId;

    public RaftNodeBehaviorFollower(RaftNode self, int term) {
        super(self, NodeStates.FOLLOWER, term);
        scheduleNextElectionTimeout();
    }

    @Override
    void close() {
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
        try {
            electionTimeout = self.getManager().schedule(this::onElectionTimeout, 150 + RaftManager.RANDOM.nextInt(151), TimeUnit.MILLISECONDS);
            LOGGER.trace("{} Scheduled next election timeout", self.getId());
        } catch (RejectedExecutionException ex) {
            if (!self.getManager().isShutdown()) {
                throw ex;
            }
        }
    }

    private void onElectionTimeout() {
        self.convertToCandidate(term + 1);
    }

    @Override
    Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        if (voteRequest.getTerm() < term) {
            LOGGER.warn("{} Received a vote request from {} for a term lower than current term: {} vs {}", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm(), term);
            return Optional.empty();
        }
        if (voteRequest.getTerm() == term && votedForNodeId != null) {
            LOGGER.warn("{} Received a vote request from {} for term {} but already voted for {}", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm(), votedForNodeId);
            return Optional.empty();
        }
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and will convert to follower (from term {})", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm(), term);
            return self.convertToFollower(voteRequest.getTerm())
                    .onVoteRequest(remote, voteRequest);
        }

        // If the receiving node hasn't voted yet in this term then it votes for the candidate...
        // ...and the node resets its election timeout.
        if (leaderId != null) {
            LOGGER.info("{} Removing current leader {}", self.getId(), leaderId);
            leaderId = null; // Remove the current leader
        }

        final TermIndex lastReceivedIndex = self.getLastReceivedIndex();
        final TermIndex committedIndex = self.getLogs().getCommittedTermIndex();
        final TermIndex incomingTermIndex = new TermIndex(
                voteRequest.getLastLogTermIndex().getTerm(), voteRequest.getLastLogTermIndex().getIndex());
        final boolean grantVote = incomingTermIndex.compareTo(lastReceivedIndex) >= 0;
        votedForNodeId = grantVote ? remote.getRemoteNodeId() : self.getId(); // Vote for self instead
        LOGGER.info("{} Voting in term {} for {} w/ grant {} (index {} vs {})", self.getId(), term,
                votedForNodeId, grantVote, voteRequest.getLastLogTermIndex(), lastReceivedIndex);
        // Voting resets the election timeout to let the voting process settle
        scheduleNextElectionTimeout();
        return Optional.of(new VoteResponse(
                voteRequest.getTerm(),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedIndex.getTerm(), committedIndex.getIndex()),
                grantVote));
    }

    @Override
    void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but will stay follower of term {}", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remote.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(remote, appendEntries);
        }

        final TermIndex currentTermIndex = self.getLogs().getCurrentTermIndex();
        final com.github.jocull.raftcache.lib.raft.messages.TermIndex currentTermIndexMessage = new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                currentTermIndex.getTerm(), currentTermIndex.getIndex());
        if (appendEntries.getTerm() < term) {
            LOGGER.warn("{} Received append entries from {} for a term lower than current term: {} vs {}", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
            return new AcknowledgeEntries(term, false, currentTermIndexMessage);
        }

        final TermIndex incomingTermIndex = new TermIndex(appendEntries.getPreviousLogTermIndex().getTerm(), appendEntries.getPreviousLogTermIndex().getIndex());
        if (!self.getLogs().containsStartPoint(incomingTermIndex)) {
            // TODO: A chance to get stuck here? What happens if indexes get out of sync?
            //       How should we reset? Will the election timeout take care of it?
            LOGGER.warn("{} Received append entries from {} term {} with invalid index: {} vs {}", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), appendEntries.getPreviousLogTermIndex(), self.getLogs().getCurrentTermIndex());
            return new AcknowledgeEntries(term, false, currentTermIndexMessage);
        }
        if (leaderId != null && !leaderId.equals(remote.getRemoteNodeId())) {
            // TODO: Does this matter...?
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", self.getId(), remote.getRemoteNodeId(), leaderId);
            return new AcknowledgeEntries(term, false, currentTermIndexMessage);
        }

        // TODO: Move this code to self.convertToFollowerForNewLeader ?
        if (leaderId == null) {
            leaderId = remote.getRemoteNodeId();
            LOGGER.info("{} Made follower of {}", self.getId(), remote.getRemoteNodeId());
        }

        // Append the logs
        if (!appendEntries.getEntries().isEmpty()) {
            final com.github.jocull.raftcache.lib.raft.messages.TermIndex newEndTermIndexMessage = appendEntries.getEntries().get(appendEntries.getEntries().size() - 1).getTermIndex();
            final TermIndex newEndTermIndex = new TermIndex(newEndTermIndexMessage.getTerm(), newEndTermIndexMessage.getIndex());
            LOGGER.debug("{} Received entries from {} for index {} -> {} with {} entries @ term {}, commit {}, {}",
                    self.getId(),
                    remote.getRemoteNodeId(),
                    currentTermIndexMessage,
                    newEndTermIndex,
                    appendEntries.getEntries().size(),
                    appendEntries.getTerm(),
                    self.getLogs().getCommittedTermIndex(),
                    appendEntries.getLeaderCommitTermIndex());
        } else {
            LOGGER.debug("{} Received heartbeat from {} @ term {}", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm());
        }

        // TODO: Probably not efficient - pass entire set of new logs and insert them in sequence instead?
        appendEntries.getEntries().forEach(r -> {
            final TermIndex newTermIndex = new TermIndex(r.getTermIndex().getTerm(), r.getTermIndex().getIndex());
            self.getLogs().appendLog(newTermIndex, r.getEntry());
        });

        // Align the commit index with the leader
        final TermIndex previousCommitIndex = self.getLogs().getCommittedTermIndex();
        final TermIndex leaderCommitIndex = new TermIndex(
                appendEntries.getLeaderCommitTermIndex().getTerm(),
                appendEntries.getLeaderCommitTermIndex().getIndex());
        if (leaderCommitIndex.compareTo(previousCommitIndex) > 0) {
            final List<RaftLog<?>> committedLogs = self.getLogs().commit(leaderCommitIndex);
            self.getManager().getEventBus().publish(new LogsCommitted(committedLogs));
            LOGGER.debug("{} Move commit index forward {} -> {} @ term {}", self.getId(), previousCommitIndex, appendEntries.getLeaderCommitTermIndex(), appendEntries.getTerm());
        }

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
        return new AcknowledgeEntries(term, true, currentTermIndexMessage);
    }

    @Override
    void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm());
            return;
        }

        LOGGER.warn("{} Received acknowledge entries from {} for term {} but doesn't make sense as follower of term {}", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
    }
}
