package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

class RaftNodeBehaviorCandidate extends RaftNodeBehavior {
    private CompletableFuture<?> electionTimeout;
    private final Set<String> votes = new HashSet<>();

    public RaftNodeBehaviorCandidate(RaftNodeImpl self, int term) {
        super(self, NodeStates.CANDIDATE, term);

        // After the election timeout the follower becomes a candidate and starts a new election term...
        // ...and sends out Request Vote messages to other nodes.
        //
        // Per Raft §5.4.1 the candidate reports the term and index of its LAST log entry.
        // §5.4.2 (up-to-date rule) says a voter grants its vote iff the candidate's last entry
        // is at least as up-to-date as the voter's own (higher term, or equal term and higher
        // or equal index). Report the last entry — NOT the committed index: the committed
        // index is always at or below the last entry, so the committed-based report would be
        // strictly more conservative and could refuse votes §5.4.1 permits to a candidate whose
        // trailing uncommitted entries are all of an OLD term (a liveness deviation in the
        // split-brain case; never a safety deviation, since committed entries only exist when
        // a quorum held them, which bounds how far behind it can be).
        final TermIndex lastLogTermIndex = self.logs.getCurrentTermIndex();
        final VoteRequest voteRequest = new VoteRequest(term, self.getId(), new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                lastLogTermIndex.getTerm(),
                lastLogTermIndex.getIndex()));

        LOGGER.info("{} Candidate starting a new election at term {}", self.getId(), term);
        votes.add(self.getId()); // ...votes for itself...

        if (votes.size() >= self.clusterTopology.getMajorityCount()) {
            // A node's own vote can only constitute a majority in a one-node cluster
            // (1 >= 1, but 1 < 2 for any larger cluster). No peer will ever send a vote
            // response, so without this check the node would loop candidate -> candidate
            // forever and never serve. Defer the conversion to the mailbox so it runs
            // after this construction finishes (convertToCandidate is in a nested call
            // on the same thread when the timeout fires) instead of re-entering mid-construction.
            LOGGER.info("{} Own vote is a majority ({} of {}) - electing self", self.getId(), votes.size(), self.clusterTopology.getMajorityCount());
            CompletableFuture.runAsync(() -> {
                if (isTerminated()) {
                    return;
                }
                self.convertToLeader();
            }, self.nodeExecutor);
            return;
        }

        synchronized (self.getActiveConnections()) {
            self.getActiveConnections().forEach(c -> c.sendVoteRequest(voteRequest));
        }

        // The election timeout is randomized to be between 150ms and 300ms.
        // For candidates, there will not be a repeat of this event. Instead, the
        // node will become a fresh candidate in the next term instead.
        //
        // The election timeout runs while we wait for responses back.
        final int timeoutMs = 150 + this.self.random.nextInt(151);
        final Executor executor = CompletableFuture.delayedExecutor(timeoutMs, TimeUnit.MILLISECONDS, this.self.nodeExecutor);
        electionTimeout = CompletableFuture.runAsync(this::onElectionTimeout, executor);
        LOGGER.trace("{} Scheduled next election timeout ({} ms)", this.self.getId(), timeoutMs);
    }

    @Override
    void closeInternal() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
            electionTimeout = null;
        }
    }

    private void onElectionTimeout() {
        // By synchronizing the timeout threads on the same instance as the exterior behavior delegates we can avoid
        // timer threads racing with internal actions that would otherwise be serialized.
        // See election timeouts in follow behavior for a stronger example of the problems this can cause.
        synchronized (self) {
            if (isTerminated()) {
                return;
            }
            LOGGER.info("{} Election timeout passed with {}/{} votes - Starting a new election", self.getId(), votes.size(), self.clusterTopology.getClusterCount());
            self.convertToCandidate(term + 1);
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

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as candidate of term {}", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), term);
    }

    @Override
    public void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received vote response from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm(), Function.identity());
            return;
        }
        if (voteResponse.getTerm() < term) {
            LOGGER.warn("{} Received vote response from {} for term {} and will ignore because term is {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            return;
        }

        if (!voteResponse.isVoteGranted()) {
            // The voter evaluated the up-to-date rule (Raft §5.4.2) against its own log and
            // refused the vote. Counting the response nonetheless would let a stale candidate
            // reach a majority out of denials and step up as leader.
            LOGGER.info("{} Received vote response from {} for term {} but the vote was not granted - not counting", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm());
            return;
        }

        final int newVoteSize;
        synchronized (votes) {
            votes.add(sender.getRemoteNodeId());
            newVoteSize = votes.size();
        }
        LOGGER.info("{} Received vote response from {} for term {} - new vote count {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), newVoteSize);
        if (newVoteSize >= self.clusterTopology.getMajorityCount()) {
            self.convertToLeader();
            LOGGER.info("{} Became leader of term {} with {}/{} votes", self.getId(), voteResponse.getTerm(), newVoteSize, self.clusterTopology.getClusterCount());
        }
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
        // "If AppendEntries RPC received from new leader: convert to follower"
        if (appendEntries.getTerm() >= term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
            self.convertToFollowerForNewLeader(sender.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(sender, appendEntries);
            return;
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as candidate of term {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
        final TermIndex current = self.logs.getCurrentTermIndex();
        final AcknowledgeEntries response = new AcknowledgeEntries(term, false, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                current.getTerm(),
                current.getIndex()));
        sender.sendAcknowledgeEntries(response);
    }

    @Override
    public void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm(), Function.identity());
            return;
        }

        LOGGER.warn("{} Received acknowledge entries from {} for term {} but doesn't make sense as candidate of term {}", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
    }
}
