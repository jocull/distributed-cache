package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            heartbeatTimeout = null;
        }
    }

    private AppendEntries.RaftLog transformLog(RaftLog<?> r) {
        return new AppendEntries.RaftLog(
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(r.getTermIndex().getTerm(), r.getTermIndex().getIndex()),
                r.getEntry());
    }

    private void onHeartbeatTimeout(boolean initial) {
        if (initial) {
            LOGGER.debug("{} Initial heartbeat", self.getId());
        } else {
            LOGGER.debug("{} Heartbeat interval fired", self.getId());
        }

        synchronized (self.getActiveConnections()) {
            final TermIndex committedTermIndex = self.getLogs().getCommittedTermIndex();
            final com.github.jocull.raftcache.lib.raft.messages.TermIndex committedTermIndexMessage =
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedTermIndex.getTerm(), committedTermIndex.getIndex());
            self.getActiveConnections().forEach(c -> {
                final List<AppendEntries.RaftLog> entries = self.getLogs().getLogRange(c.getCurrentTermIndex(), 25, this::transformLog);
                if (!entries.isEmpty()) {
                    final com.github.jocull.raftcache.lib.raft.messages.TermIndex newEndTermIndex = entries.get(entries.size() - 1).getTermIndex();
                    LOGGER.debug("{} Appending entries to {} setting index {} -> {} with {} entries @ term {}", self.getId(), c.getRemoteNodeId(), c.getCurrentTermIndex(), newEndTermIndex, entries.size(), self.getCurrentTerm());
                } else {
                    LOGGER.debug("{} Sending heartbeat to {}", self.getId(), c.getRemoteNodeId());
                }
                c.appendEntries(new AppendEntries(
                        self.getCurrentTerm(),
                        new com.github.jocull.raftcache.lib.raft.messages.TermIndex(c.getCurrentTermIndex().getTerm(), c.getCurrentTermIndex().getIndex()),
                        committedTermIndexMessage,
                        entries));
            });
        }

        // Chain to the next heartbeat
        try {
            heartbeatTimeout = self.getManager().schedule(() -> this.onHeartbeatTimeout(false), 50, TimeUnit.MILLISECONDS);
            LOGGER.trace("{} Scheduled next heartbeat", self.getId());
        } catch (RejectedExecutionException ex) {
            if (!self.getManager().isShutdown()) {
                throw ex;
            }
        }
    }

    private void updateCommittedIndex() {
        final List<TermIndex> currentIndices;
        synchronized (self.getActiveConnections()) {
            currentIndices = self.getActiveConnections().stream()
                    .map(NodeCommunication::getCurrentTermIndex)
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        }

        final int majorityCount = self.getClusterTopology().getMajorityCount();
        if (currentIndices.size() >= majorityCount) {
            final TermIndex majorityMinimumIndex = currentIndices.get(majorityCount - 1);
            // TODO: Is this safe? Is there a chance that responding entries could exceed the leader's current index/term?
            //       Some kind of term shuffling that would cause chaos?
            if (majorityMinimumIndex.compareTo(self.getLogs().getCommittedTermIndex()) > 0) {
                LOGGER.debug("{} Has minimum majority index {} to commit", self.getId(), majorityMinimumIndex);
                final List<RaftLog<?>> committedLogs = self.getLogs().commit(majorityMinimumIndex);
                self.getManager().getEventBus().publish(new LogsCommitted(committedLogs));
            }
        } else {
            LOGGER.debug("{} Not a majority to commit with {} (need {})", self.getId(), currentIndices, majorityCount);
        }
    }

    @Override
    Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and granted vote as new follower", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm());
            return self.convertToFollower(voteRequest.getTerm())
                    .onVoteRequest(remote, voteRequest);
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as leader of term {}", self.getId(), remote.getRemoteNodeId(), voteRequest.getTerm(), term);
        return Optional.empty();
    }

    @Override
    void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but already leader of term {}", self.getId(), remote.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
            return self.convertToFollowerForNewLeader(remote.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(remote, appendEntries);
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as leader of term {}", self.getId(), remote.getRemoteNodeId(), appendEntries.getTerm(), term);
        final TermIndex currentTermIndex = self.getLogs().getCurrentTermIndex();
        return new AcknowledgeEntries(term, false, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                currentTermIndex.getTerm(),
                currentTermIndex.getIndex()));
    }

    @Override
    void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm());
            return;
        }
        if (!acknowledgeEntries.isSuccess()) {
            LOGGER.warn("{} received AcknowledgeEntries without success from {}: {}, {} @ term {}", self.getId(), remote.getRemoteNodeId(), acknowledgeEntries.getTerm(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
            return;
        }

        LOGGER.debug("{} received AcknowledgeEntries from {} moving index {} -> {} @ term {}", self.getId(), remote.getRemoteNodeId(), remote.getCurrentTermIndex(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
        remote.setCurrentTermIndex(new TermIndex(
                acknowledgeEntries.getCurrentTermIndex().getTerm(),
                acknowledgeEntries.getCurrentTermIndex().getIndex()));
        updateCommittedIndex();
    }
}
