package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.events.LogsCommitted;
import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RaftNodeBehaviorFollower extends RaftNodeBehavior {
    private volatile ScheduledFuture<?> electionTimeout;
    private volatile String votedForNodeId;

    public RaftNodeBehaviorFollower(RaftNode self, int term) {
        super(self, NodeStates.FOLLOWER, term);
        scheduleNextElectionTimeout();
    }

    @Override
    void close() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
        }
    }

    private void scheduleNextElectionTimeout() {
        if (electionTimeout != null
                && !electionTimeout.isCancelled()
                && !electionTimeout.isDone()) {
            electionTimeout.cancel(false);
        }
        // The election timeout is randomized to be between 150ms and 300ms.
        electionTimeout = self.getManager().schedule(this::onElectionTimeout, 150 + RaftManager.RANDOM.nextInt(151), TimeUnit.MILLISECONDS);
    }

    private synchronized void onElectionTimeout() {
        self.convertToCandidate(term + 1);
    }

    @Override
    Optional<VoteResponse> onVoteRequest(String remoteNodeId, VoteRequest voteRequest) {
        if (voteRequest.getTerm() < term) {
            LOGGER.warn("{} Received a vote request from {} for a term lower than current term: {} vs {}", self.getId(), remoteNodeId, voteRequest.getTerm(), term);
            return Optional.empty();
        }
        if (voteRequest.getTerm() == term && votedForNodeId != null) {
            LOGGER.warn("{} Received a vote request from {} for term {} but already voted for {}", self.getId(), remoteNodeId, voteRequest.getTerm(), votedForNodeId);
            return Optional.empty();
        }
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will begin a new active election", self.getId(), remoteNodeId, voteRequest.getTerm());
            return self.convertToFollowerWithVote(term, remoteNodeId);
        }

        // If the receiving node hasn't voted yet in this term then it votes for the candidate...
        // ...and the node resets its election timeout.
        if (leaderId != null) {
            LOGGER.info("{} Removing current leader {}", self.getId(), leaderId);
            leaderId = null; // Remove the current leader
        }

        final long lastReceivedIndex = self.getLastReceivedIndex();
        final boolean grantVote = voteRequest.getLastLogIndex() >= lastReceivedIndex;
        votedForNodeId = grantVote ? remoteNodeId : self.getId(); // Vote for self instead
        LOGGER.info("{} Voting in term {} for {} w/ grant {} (index {} vs {})", self.getState(), term,
                votedForNodeId, grantVote, voteRequest.getLastLogIndex(), lastReceivedIndex);
        // Voting resets the election timeout to let the voting process settle
        scheduleNextElectionTimeout();
        return Optional.of(new VoteResponse(voteRequest.getTerm(), grantVote));
    }

    @Override
    void onVoteResponse(String remoteNodeId, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), remoteNodeId, voteResponse.getTerm(), term);
            self.convertToFollower(term);
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but will stay follower of term {}", self.getId(), remoteNodeId, voteResponse.getTerm(), term);
    }

    @Override
    AcknowledgeEntries onAppendEntries(String remoteNodeId, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remoteNodeId, appendEntries);
        }
        if (appendEntries.getTerm() < term) {
            LOGGER.warn("{} Received append entries from {} for a term lower than current term: {} vs {}", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
            return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
        }
        if (!self.getLogs().containsStartPoint(appendEntries.getTerm(), appendEntries.getPreviousLogIndex())) {
            // TODO: A chance to get stuck here? What happens if indexes get out of sync?
            //       How should we reset? Will the election timeout take care of it?
            LOGGER.warn("{} Received append entries from {} term {} with invalid index: {} vs {}", self.getId(), remoteNodeId, appendEntries.getTerm(), appendEntries.getPreviousLogIndex(), self.getLogs().getCurrentIndex());
            return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
        }
        if (leaderId != null && !leaderId.equals(remoteNodeId)) {
            // TODO: Does this matter...?
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", self.getId(), remoteNodeId, leaderId);
            return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
        }

        // TODO: Move this code to self.convertToFollowerForNewLeader ?
        if (leaderId == null) {
            leaderId = remoteNodeId;
            LOGGER.info("{} Made follower of {}", self.getId(), remoteNodeId);
        }

        // Append the logs
        if (!appendEntries.getEntries().isEmpty()) {
            final long newEndIndex = appendEntries.getEntries().get(appendEntries.getEntries().size() - 1).getIndex();
            LOGGER.debug("{} Received entries from {} for index {} -> {} with {} entries", self.getId(), remoteNodeId, self.getLogs().getCurrentIndex(), newEndIndex, appendEntries.getEntries().size());
        } else {
            LOGGER.debug("{} Received heartbeat from {}", self.getId(), remoteNodeId);
        }
        appendEntries.getEntries().forEach(r -> self.getLogs().appendLog(appendEntries.getTerm(), r.getIndex(), r.getEntry()));

        // Align the commit index with the leader
        final List<RaftLog<?>> committedLogs = self.getLogs().commit(appendEntries.getLeaderCommitIndex());
        self.getManager().getEventBus().publish(new LogsCommitted(committedLogs));

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
        return new AcknowledgeEntries(term, true, self.getLogs().getCurrentIndex());
    }

    @Override
    void onAcknowledgeEntries(String remoteNodeId, AcknowledgeEntries acknowledgeEntries) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
