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
            self.convertToFollower(voteRequest.getTerm(), f -> {
                f.onVoteRequest(sender, voteRequest);
                return null;
            });
            return;
        }

        // If the receiving node hasn't voted yet in this term then it votes for the candidate...
        // ...and the node resets its election timeout.
        if (leaderId != null) {
            LOGGER.info("{} Removing current leader {}", self.getId(), leaderId);
            leaderId = null; // Remove the current leader
        }

        final TermIndex currentIndex = self.logs.getCurrentTermIndex();
        final TermIndex committedIndex = self.logs.getCommittedTermIndex();
        final TermIndex incomingTermIndex = new TermIndex(
                voteRequest.getLastLogTermIndex().getTerm(), voteRequest.getLastLogTermIndex().getIndex());
        final boolean grantVote = incomingTermIndex.compareTo(currentIndex) >= 0;
        votedForNodeId = grantVote ? sender.getRemoteNodeId() : self.getId(); // Vote for self instead
        LOGGER.info("{} Voting in term {} for {} w/ grant {} (index {} vs {})", self.getId(), term,
                votedForNodeId, grantVote, voteRequest.getLastLogTermIndex(), currentIndex);
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
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(appendEntries, term, false, currentTermIndexMessage));
            return;
        }

        final TermIndex incomingTermIndex = new TermIndex(appendEntries.getPreviousLogTermIndex().getTerm(), appendEntries.getPreviousLogTermIndex().getIndex());
        if (!self.logs.containsStartPoint(incomingTermIndex)) {
            // TODO: A chance to get stuck here? What happens if indexes get out of sync?
            //       How should we reset? Will the election timeout take care of it?
            LOGGER.warn("{} Received append entries from {} term {} with invalid index: {} vs {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), appendEntries.getPreviousLogTermIndex(), self.logs.getCurrentTermIndex());
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(appendEntries, term, false, currentTermIndexMessage));
            return;
        }
        if (leaderId != null && !leaderId.equals(sender.getRemoteNodeId())) {
            // TODO: Does this matter...?
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", self.getId(), sender.getRemoteNodeId(), leaderId);
            sender.sendAcknowledgeEntries(new AcknowledgeEntries(appendEntries, term, false, currentTermIndexMessage));
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
            self.eventBus.publish(new LogsCommitted(committedLogs));
            LOGGER.debug("{} Move commit index forward {} -> {} @ term {}", self.getId(), previousCommitIndex, appendEntries.getLeaderCommitTermIndex(), appendEntries.getTerm());
        }

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
        sender.sendAcknowledgeEntries(new AcknowledgeEntries(appendEntries, term, true, currentTermIndexMessage));
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
