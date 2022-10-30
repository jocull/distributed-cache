package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class RaftNodeBehaviorLeader extends RaftNodeBehavior {
    private volatile ScheduledFuture<?> heartbeatTimeout;

    public RaftNodeBehaviorLeader(RaftNode self, int term) {
        super(self, NodeStates.LEADER, term);

        onHeartbeatTimeout(true);
    }

    @Override
    void close() {
        if (heartbeatTimeout != null) {
            heartbeatTimeout.cancel(false);
        }
    }

    private AppendEntries.RaftLog transformLog(RaftLog<?> r) {
        return new AppendEntries.RaftLog(r.getIndex(), r.getEntry());
    }

    private void onHeartbeatTimeout(boolean initial) {
        if (initial) {
            LOGGER.debug("{} Initial heartbeat", self.getId());
        } else {
            LOGGER.debug("{} Heartbeat interval fired", self.getId());
        }

        synchronized (self.getActiveConnections()) {
            self.getActiveConnections().forEach(c -> {
                final List<AppendEntries.RaftLog> entries = self.getLogs().getLogRange(c.getCurrentIndex(), 25, this::transformLog);
                if (!entries.isEmpty()) {
                    final long newEndIndex = entries.get(entries.size() - 1).getIndex();
                    LOGGER.debug("{} Appending entries to {} setting index {} -> {} with {} entries", self.getId(), c.getRemoteNodeId().orElseThrow(), c.getCurrentIndex(), newEndIndex, entries.size());
                } else {
                    LOGGER.debug("{} Sending heartbeat to {}", self.getId(), c.getRemoteNodeId().orElseThrow());
                }
                c.appendEntries(new AppendEntries(
                        self.getCurrentTerm(),
                        c.getCurrentIndex(),
                        self.getLogs().getCommitIndex(),
                        entries));
            });
        }

        // Chain to the next heartbeat
        heartbeatTimeout = self.getManager().schedule(() -> this.onHeartbeatTimeout(false), 50, TimeUnit.MILLISECONDS);
        LOGGER.debug("{} Scheduled next heartbeat", self.getId());
    }

    @Override
    Optional<VoteResponse> onVoteRequest(String remoteNodeId, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will begin a new active election", self.getId(), remoteNodeId, voteRequest.getTerm());
            return self.convertToFollowerWithVote(term, remoteNodeId);
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as leader of term {}", self.getId(), remoteNodeId, voteRequest.getTerm(), term);
        return Optional.empty();
    }

    @Override
    void onVoteResponse(String remoteNodeId, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), remoteNodeId, voteResponse.getTerm(), term);
            self.convertToFollower(term);
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but already leader of term {}", self.getId(), remoteNodeId, voteResponse.getTerm(), term);
    }

    @Override
    AcknowledgeEntries onAppendEntries(String remoteNodeId, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remoteNodeId, appendEntries);
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as leader of term {}", self.getId(), remoteNodeId, appendEntries.getTerm(), term);
        return new AcknowledgeEntries(term, false, self.getLogs().getCurrentIndex());
    }

    @Override
    void onAcknowledgeEntries(String remoteNodeId, AcknowledgeEntries acknowledgeEntries) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
