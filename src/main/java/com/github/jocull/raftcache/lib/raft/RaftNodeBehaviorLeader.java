package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class RaftNodeBehaviorLeader extends RaftNodeBehavior {
    private CompletableFuture<?> heartbeatTimeout;

    public RaftNodeBehaviorLeader(RaftNodeImpl self, int term) {
        super(self, NodeStates.LEADER, term);

        onHeartbeatTimeout(true);
    }

    @Override
    void closeInternal() {
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
        if (isTerminated()) {
            return;
        }

        if (initial) {
            LOGGER.debug("{} Initial heartbeat", self.getId());
        } else {
            LOGGER.debug("{} Heartbeat interval fired", self.getId());
        }
        synchronized (self.getActiveConnections()) {
            final TermIndex committedTermIndex = self.logs.getCommittedTermIndex();
            final com.github.jocull.raftcache.lib.raft.messages.TermIndex committedTermIndexMessage =
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedTermIndex.getTerm(), committedTermIndex.getIndex());
            self.getActiveConnections().forEach(c -> {
                final List<AppendEntries.RaftLog> entries = self.logs.getLogRange(c.getTermIndex(), 25, this::transformLog);
                if (!entries.isEmpty()) {
                    final com.github.jocull.raftcache.lib.raft.messages.TermIndex newEndTermIndex = entries.get(entries.size() - 1).getTermIndex();
                    LOGGER.debug("{} Appending entries to {} setting index {} -> {} with {} entries @ term {}", self.getId(), c.getRemoteNodeId(), c.getTermIndex(), newEndTermIndex, entries.size(), term);
                } else {
                    LOGGER.debug("{} Sending heartbeat to {}", self.getId(), c.getRemoteNodeId());
                }
                c.sendAppendEntries(new AppendEntries(
                        term,
                        new com.github.jocull.raftcache.lib.raft.messages.TermIndex(c.getTermIndex().getTerm(), c.getTermIndex().getIndex()),
                        committedTermIndexMessage,
                        entries));
            });
        }

        // Chain to the next heartbeat
        final int timeoutMs = 50;
        final Executor executor = CompletableFuture.delayedExecutor(timeoutMs, TimeUnit.MILLISECONDS, this.self.nodeExecutor);
        heartbeatTimeout = CompletableFuture.runAsync(() -> this.onHeartbeatTimeout(false), executor);
        LOGGER.trace("{} Scheduled next heartbeat ({} ms)", this.self.getId(), timeoutMs);
    }

    private void updateCommittedIndex() {
        final List<TermIndex> currentIndices;
        synchronized (self.getActiveConnections()) {
            currentIndices = self.getActiveConnections().stream()
                    .map(NodeConnectionOutbound::getTermIndex)
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        }

        final int majorityCount = self.clusterTopology.getMajorityCount();
        if (currentIndices.size() >= majorityCount) {
            final TermIndex majorityMinimumIndex = currentIndices.get(majorityCount - 1);
            // TODO: Is this safe? Is there a chance that responding entries could exceed the leader's current index/term?
            //       Some kind of term shuffling that would cause chaos?
            if (majorityMinimumIndex.compareTo(self.logs.getCommittedTermIndex()) > 0) {
                LOGGER.debug("{} Has minimum majority index {} to commit", self.getId(), majorityMinimumIndex);
                final List<RaftLog<?>> committedLogs = self.logs.commit(majorityMinimumIndex);
                self.eventBus.publish(new LogsCommitted(committedLogs));
            }
        } else {
            LOGGER.debug("{} Not a majority to commit with {} (need {})", self.getId(), currentIndices, majorityCount);
        }
    }

    @Override
    public void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest) {
        if (voteRequest.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} and granted vote as new follower", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm());
            self.convertToFollower(voteRequest.getTerm(), f -> {
                f.onVoteRequest(sender, voteRequest);
                return null;
            });
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but won't vote as leader of term {}", self.getId(), sender.getRemoteNodeId(), voteRequest.getTerm(), term);
    }

    @Override
    public void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse) {
        if (voteResponse.getTerm() > term) {
            LOGGER.info("{} Received a vote request from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
            self.convertToFollower(voteResponse.getTerm(), Function.identity());
            return;
        }

        LOGGER.info("{} Received a vote request from {} for term {} but already leader of term {}", self.getId(), sender.getRemoteNodeId(), voteResponse.getTerm(), term);
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
        if (appendEntries.getTerm() > term) {
            LOGGER.info("{} Received append entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
            self.convertToFollowerForNewLeader(sender.getRemoteNodeId(), appendEntries)
                    .onAppendEntries(sender, appendEntries);
            return;
        }

        LOGGER.info("{} Received append entries from {} for term {} but won't succeed as leader of term {}", self.getId(), sender.getRemoteNodeId(), appendEntries.getTerm(), term);
        final TermIndex currentTermIndex = self.logs.getCurrentTermIndex();
        final AcknowledgeEntries response = new AcknowledgeEntries(term, false, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                currentTermIndex.getTerm(),
                currentTermIndex.getIndex()));

        sender.sendAcknowledgeEntries(response);
    }

    @Override
    public void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries) {
        if (acknowledgeEntries.getTerm() > term) {
            LOGGER.info("{} Received acknowledge entries from {} for term {} will move from term {} as follower", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), term);
            self.convertToFollower(acknowledgeEntries.getTerm(), Function.identity());
            return;
        }
        if (!acknowledgeEntries.isSuccess()) {
            LOGGER.warn("{} received AcknowledgeEntries without success from {}: {}, {} @ term {}", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
            return;
        }

        LOGGER.debug("{} received AcknowledgeEntries from {} moving index {} -> {} @ term {}", self.getId(), sender.getRemoteNodeId(), sender.getTermIndex(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
        sender.setTermIndex(new TermIndex(
                acknowledgeEntries.getCurrentTermIndex().getTerm(),
                acknowledgeEntries.getCurrentTermIndex().getIndex()));
        updateCommittedIndex();
    }
}
