package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;

import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RaftNodeBehaviorCandidate extends RaftNodeBehavior {
    private volatile ScheduledFuture<?> electionTimeout;
    private int voteCount;

    public RaftNodeBehaviorCandidate(RaftNode self, int term) {
        super(self, NodeStates.CANDIDATE, term);

        // After the election timeout the follower becomes a candidate and starts a new election term...
        // ...and sends out Request Vote messages to other nodes.
        final int previousTerm = term - 1;
        final long lastCommittedLogIndex = self.getLogs().getCommitIndex();
        final VoteRequest voteRequest = new VoteRequest(term, self.getId(), lastCommittedLogIndex, previousTerm);

        LOGGER.info("{} Candidate starting a new election at term {}", self.getId(), term);
        voteCount = 1; // ...votes for itself...
        synchronized (self.getActiveConnections()) {
            self.getActiveConnections().forEach(c -> c.requestVote(voteRequest));
        }

        // The election timeout is randomized to be between 150ms and 300ms.
        // For candidates, there will not be a repeat of this event. Instead, the
        // node will become a fresh candidate in the next term instead.
        //
        // The election timeout runs while we wait for responses back.
        electionTimeout = self.getManager().schedule(this::onElectionTimeout, 150 + RaftManager.RANDOM.nextInt(151), TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    Optional<VoteResponse> onVoteRequest(String remoteNodeId, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will begin a new active election", self.getId(), remoteNodeId, voteRequest.getTerm());
            return self.convertToFollowerWithVote(term, remoteNodeId);
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as candidate of term {}", self.getId(), remoteNodeId, voteRequest.getTerm(), term);
        return Optional.empty();
    }

    @Override
    void onVoteResponse(String remoteNodeId, VoteResponse voteResponse) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    AcknowledgeEntries onAppendEntries(String remoteNodeId, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remoteNodeId, appendEntries);
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as candidate of term {}", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
        return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
    }

    @Override
    void onAcknowledgeEntries(String remoteNodeId, AcknowledgeEntries acknowledgeEntries) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
