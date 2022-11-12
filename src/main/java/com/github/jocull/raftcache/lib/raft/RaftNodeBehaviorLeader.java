package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.raft.events.AppendLog;
import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class RaftNodeBehaviorLeader extends RaftNodeBehavior {
    private final int heartbeatTimeoutMillis = 50;

    private CompletableFuture<?> heartbeatTimeout;

    private final EventSubscriber entriesSubscriber;

    private enum AppendEntriesTrigger {
        INITIAL,
        TIMEOUT,
        CONTINUATION,
        NEW_LOG,
        ;
    }

    public RaftNodeBehaviorLeader(RaftNodeImpl self, int term) {
        super(self, NodeStates.LEADER, term);

        entriesSubscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof AppendLog) {
                    onAppendLog((AppendLog) event);
                }
            }
        };

        // Send the initial append first thing to declare self as leader
        appendEntries(AppendEntriesTrigger.INITIAL);
    }

    @Override
    void closeInternal() {
        self.eventBus.unsubscribe(entriesSubscriber);
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

    private void onAppendLog(AppendLog appendLog) {
        if (isTerminated()) {
            return;
        }
        CompletableFuture.runAsync(() -> appendEntries(AppendEntriesTrigger.NEW_LOG), self.nodeExecutor);
    }

    private void onHeartbeatTimeout() {
        if (isTerminated()) {
            return;
        }

        LOGGER.debug("{} Heartbeat interval fired", self.getId());
        heartbeatTimeout = null; // Erase now to avoid cancelling self on reschedule
        appendEntries(AppendEntriesTrigger.TIMEOUT);
    }

    private void appendEntries(AppendEntriesTrigger trigger) {
        final TermIndex committedTermIndex = self.logs.getCommittedTermIndex();
        final com.github.jocull.raftcache.lib.raft.messages.TermIndex committedTermIndexMessage =
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(committedTermIndex.getTerm(), committedTermIndex.getIndex());

        self.activeConnections.forEach(c -> {
            if (!c.shouldSendNextEntries(heartbeatTimeoutMillis)) {
                LOGGER.debug("{} Still waiting for ack before send entries to {} - last send {}, receive {}", self.id, c.getRemoteNodeId(), c.getLastEntriesSent(), c.getLastEntriesAcknowledged());
                return;
            }

            final List<AppendEntries.RaftLog> entries = self.logs.getLogRange(c.getTermIndex(), 200, this::transformLog);
            if (entries.isEmpty() && trigger == AppendEntriesTrigger.CONTINUATION) {
                // Continuations only send them if there's entries to send, otherwise they stay quiet
                LOGGER.debug("{} No logs remaining to send on continuation to {}", self.getId(), c.getRemoteNodeId());
                return;
            }

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
            c.setLastEntriesSent(Instant.now());
        });

        // After appending, reset the next heartbeat
        if (heartbeatTimeout != null && !heartbeatTimeout.isDone()) {
            heartbeatTimeout.cancel(false);
        }

        // What should the next timeout be anyway? Find the lowest common denominator.
        // Needs to target the time that is (lastSent + timeout) from now
        final int targetTimeoutMillis = self.activeConnections.stream()
                .map(NodeCommunicationState::getLastEntriesSent)
                .min(Instant::compareTo)
                .map(minTime -> {
                    final Instant targetTime = minTime.plus(heartbeatTimeoutMillis, ChronoUnit.MILLIS);
                    final long targetMillis = Duration.between(Instant.now(), targetTime).toMillis();
                    if (targetMillis < 0) {
                        return 0;
                    }
                    if (targetMillis > heartbeatTimeoutMillis) {
                        return heartbeatTimeoutMillis;
                    }
                    return (int) targetMillis;
                })
                .orElse(0);

        final Executor delayedExecutor = CompletableFuture.delayedExecutor(targetTimeoutMillis, TimeUnit.MILLISECONDS, this.self.nodeExecutor);
        heartbeatTimeout = CompletableFuture.runAsync(this::onHeartbeatTimeout, delayedExecutor);
        LOGGER.trace("{} Scheduled next heartbeat ({} ms)", this.self.getId(), targetTimeoutMillis);
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
            LOGGER.warn("{} Received AcknowledgeEntries without success from {}: {}, {} @ term {}", self.getId(), sender.getRemoteNodeId(), acknowledgeEntries.getTerm(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
            final TermIndex previous = sender.getTermIndex();
            sender.setTermIndex(new TermIndex(
                    acknowledgeEntries.getCurrentTermIndex().getTerm(),
                    acknowledgeEntries.getCurrentTermIndex().getIndex()
            ));
            LOGGER.warn("{} Rewound {}: {} -> {}", self.id, sender.getRemoteNodeId(), previous, sender.getTermIndex());
            return;
        }

        // TODO: What happens if we get out-of-order ack's, moving the term/index backwards?
        //       Does that matter? Would it fix itself?
        LOGGER.debug("{} received AcknowledgeEntries from {} moving index {} -> {} @ term {}", self.getId(), sender.getRemoteNodeId(), sender.getTermIndex(), acknowledgeEntries.getCurrentTermIndex(), acknowledgeEntries.getTerm());
        sender.setTermIndex(new TermIndex(
                acknowledgeEntries.getCurrentTermIndex().getTerm(),
                acknowledgeEntries.getCurrentTermIndex().getIndex()));

        updateCommittedIndex();
        sender.setLastEntriesAcknowledged(Instant.now());

        // Are there more entries to send? Go ahead and send them!
        appendEntries(AppendEntriesTrigger.CONTINUATION);
    }
}
