package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RaftNodeBehaviorCandidate extends RaftNodeBehavior {
    private volatile ScheduledFuture<?> electionTimeout;
    private final Set<String> votes = new HashSet<>();

    public RaftNodeBehaviorCandidate(RaftNode self, int term) {
        super(self, NodeStates.CANDIDATE, term);

        // After the election timeout the follower becomes a candidate and starts a new election term...
        // ...and sends out Request Vote messages to other nodes.
        final int previousTerm = term - 1;
        final long lastCommittedLogIndex = self.getLogs().getCommitIndex();
        final VoteRequest voteRequest = new VoteRequest(term, self.getId(), lastCommittedLogIndex, previousTerm);

        LOGGER.info("{} Candidate starting a new election at term {}", self.getId(), term);
        votes.add(self.getId()); // ...votes for itself...
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

    @Override
    void close() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
            electionTimeout = null;
        }
    }

    private void onElectionTimeout() {
        synchronized (votes) {
            LOGGER.info("{} Election timeout passed with {}/{} votes - Starting a new election", self.getId(), votes.size(), self.getClusterTopology().getClusterCount());
        }
        self.convertToCandidate(term + 1);
    }

    @Override
    Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and granted vote as new follower", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm());
            self.convertToFollower(voteRequest.getTerm())
                    .onVoteRequest(remote, voteRequest);
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as candidate of term {}", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm(), term);
        return Optional.empty();
    }

    @Override
    void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received vote response from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm());
            return;
        }
        if (voteResponse.getTerm() < term) {
            LOGGER.warn("{} Received vote response from {} for term {} and will ignore because term is {}", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
            return;
        }

        final int newVoteSize;
        synchronized (votes) {
            votes.add(remote.getRemoteNodeId());
            newVoteSize = votes.size();
        }
        LOGGER.info("{} Received vote response from {} for term {} - new vote count {}", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), newVoteSize);
        if (newVoteSize >= self.getClusterTopology().getMajorityCount()) {
            self.convertToLeader();
            LOGGER.info("{} Became leader of term {} with {}/{} votes", self.getId(), voteResponse.getTerm(), newVoteSize, self.getClusterTopology().getClusterCount());
        }
    }

    @Override
    AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        // "If AppendEntries RPC received from new leader: convert to follower"
        if (appendEntries.getTerm() >= term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remote.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(remote, appendEntries);
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as candidate of term {}", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
        return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
    }

    @Override
    void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm());
            return;
        }

        LOGGER.warn("{} Received acknowledge entries from {} for term {} but doesn't make sense as candidate of term {}", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
    }
}
