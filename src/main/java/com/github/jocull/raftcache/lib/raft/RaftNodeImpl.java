package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.AsyncUtilities;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.ClusterTopology;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import com.github.jocull.raftcache.lib.topology.TopologyDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

class RaftNodeImpl implements RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeImpl.class);

    private static final AtomicInteger RAFT_NODE_COUNTER = new AtomicInteger();

    // Node management
    final Random random = new Random();
    final ScheduledExecutorService nodeExecutor;
    final EventBus eventBus;

    // Node details
    final String id;
    final NodeAddress nodeAddress;
    private RaftNodeBehavior behavior = new RaftNodeBehaviorFollowerInitial(this);

    // Consistent-observation pinning, touched only on the node executor thread.
    // A behavior conversion (leader step-down, campaign, ...) replaces this single
    // immutable snapshot, so any individual read of it is valid -- but two SEPARATE
    // reads (a state task, then a term task on the executor) can bracket a conversion
    // and fabricate a (LEADER, higher-term) pair that was never true at one moment.
    // getNodeState() therefore samples (state, term, index) atomically and, when the
    // node is LEADER, publishes the TermIndex here; the next getCurrentTermIndex() read
    // returns exactly that sample (and consumes it), so a caller collecting state first
    // and term after always sees one consistent (state, term, index) observation.
    private TermIndex pendingLeaderTermIndex;

    // Topology and connections
    final TopologyDiscovery topologyDiscovery;
    final ChannelMiddleware channelMiddleware;
    final ClusterTopology clusterTopology;
    final List<NodeConnectionOutbound> activeConnections = new ArrayList<>();

    // Log management
    final RaftLogs logs;

    RaftNodeImpl(String id,
                 NodeAddress nodeAddress,
                 TopologyDiscovery topologyDiscovery,
                 ChannelMiddleware channelMiddleware,
                 EventBus eventBus) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.topologyDiscovery = topologyDiscovery;
        this.channelMiddleware = channelMiddleware;
        this.eventBus = eventBus;
        this.logs = new RaftLogs(eventBus);

        // Each node maintains a scheduled executor to serialize operations and run timers
        {
            final int thisNodeCount = RAFT_NODE_COUNTER.getAndIncrement();
            final ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(
                    1,
                    r -> new Thread(r, "raft-node-" + thisNodeCount));
            scheduledExecutor.setRemoveOnCancelPolicy(true);
            scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduledExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.nodeExecutor = scheduledExecutor;
        }

        // Each node has its own view of cluster topology and self registers with it
        this.clusterTopology = new ClusterTopology();
        this.clusterTopology.register(new NodeIdentifier(id, nodeAddress));
    }

    private void startInternal() {
        // All nodes start in FOLLOWER state until they hear from a leader or start an election
        behavior.close();
        behavior = new RaftNodeBehaviorFollower(this, 0);
    }

    private void stopInternal() {
        // Stopping the node stops any timers and resets the term back to a beginning state
        behavior.close();
        behavior = new RaftNodeBehaviorFollowerInitial(this);
        nodeExecutor.shutdownNow(); // TODO: MEGA WEIRD!!!
    }

    /* package-private-for-test */ List<NodeConnectionOutbound> getActiveConnections() {
        synchronized (activeConnections) {
            return List.copyOf(activeConnections);
        }
    }

    /* package-private-for-test */ NodeCommunicationReceiver currentReceiver() {
        return behavior;
    }

    @Deprecated // TODO: this should be consolidated somewhere else
    private void rollback(int oldTerm, int newTerm) {
        final TermIndex previousIndex = logs.getCurrentTermIndex();
        final List<RaftLog<?>> rollback = logs.rollback();
        final TermIndex newIndex = logs.getCurrentTermIndex();
        LOGGER.info("{} Rolling back {} logs from term {} -> {}, index {} -> {}",
                id, rollback.size(), oldTerm, newTerm, previousIndex, newIndex);
    }

    // region Connection handling

    private boolean hasNodeConnection(NodeAddress remoteAddress) {
        return activeConnections.stream().anyMatch(c -> c.getRemoteNodeAddress().equals(remoteAddress));
    }

    private List<NodeConnectionOutbound> newTopologyConnections() {
        // TODO: Optimize.
        //  New connections could be made in the background and then joined with the main executor thread safely.
        final List<NodeAddress> nodeAddresses = topologyDiscovery.discoverNodes();
        return nodeAddresses.stream()
                .filter(remoteAddress -> {
                    // Skip self
                    if (remoteAddress.getAddress().equals(getNodeAddress().getAddress())) {
                        return false;
                    }
                    // Skip nodes already connected with
                    if (hasNodeConnection(remoteAddress)) {
                        LOGGER.debug("{} Already has connection to {} - skipping", id, remoteAddress.getAddress());
                        return false;
                    }
                    return true;
                })
                .map(this::newConnection)
                .collect(Collectors.toList());
    }

    private NodeConnectionOutbound newConnection(NodeAddress remoteAddress) {
        // TODO: Optimize.
        //  New connections could be made in the background and then joined with the main executor thread safely.
        if (hasNodeConnection(remoteAddress)) {
            throw new IllegalStateException("Node " + remoteAddress.getAddress() + " already has a connection");
        }

        LOGGER.debug("{} Establishing connection to {}", id, remoteAddress.getAddress());
        final NodeConnectionOutbound connection = new NodeConnectionOutboundImpl(
                fnReceiver -> CompletableFuture.runAsync(() -> fnReceiver.accept(behavior), nodeExecutor),
                channelMiddleware.openChannel(this, remoteAddress));
        activeConnections.add(connection);

        final TermIndex currentTermIndex = logs.getCurrentTermIndex();
        connection.sendIntroduction(new Introduction(
                id,
                nodeAddress,
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        currentTermIndex.getTerm(),
                        currentTermIndex.getIndex())));

        return connection;
    }

    private NodeConnectionOutbound onNewConnection(ChannelMiddleware.ChannelSide incoming) {
        LOGGER.debug("{} Incoming connection from {}", id, incoming.getAddress());
        final NodeConnectionOutbound connection = new NodeConnectionOutboundImpl(
                fnReceiver -> CompletableFuture.runAsync(() -> fnReceiver.accept(behavior), nodeExecutor),
                incoming);
        activeConnections.add(connection);
        return connection;
    }

    private void disconnectFromInternal(NodeConnectionOutbound connection) {
        LOGGER.debug("{} Disconnecting from {}", id, connection.getRemoteNodeAddress().getAddress());
        throw new UnsupportedOperationException("Disconnect not yet implemented!");
    }

    // endregion

    // region Node type conversions

    <TOut> TOut convertToFollower(int newTerm, Function<RaftNodeBehaviorFollower, TOut> fnActionWithLock) {
        behavior.close();
        final int oldTerm = behavior.term;
        rollback(oldTerm, newTerm);

        behavior = new RaftNodeBehaviorFollower(this, newTerm);
        return fnActionWithLock.apply((RaftNodeBehaviorFollower) behavior); // Do this action while the lock is held!
    }

    /**
     * Convert to a follower to vote on an incoming VoteRequest: the term advances but the log
     * is deliberately NOT rolled back. A VoteRequest carries no log contents, and per Raft
     * §5.4.1 the up-to-date rule must be evaluated against the voter's CURRENT (unmutated)
     * log: a rollback here would discard the voter's uncommitted tail (e.g. entries
     * replicated but not yet committed at partition time), turning a strictly-later log into
     * a tie and letting a stale candidate's vote be granted. This matches convertToCandidate,
     * which already omits the rollback for the same reason.
     */
    <TOut> TOut convertToFollowerForVote(int newTerm, Function<RaftNodeBehaviorFollower, TOut> fnActionWithLock) {
        behavior.close();
        behavior = new RaftNodeBehaviorFollower(this, newTerm);
        return fnActionWithLock.apply((RaftNodeBehaviorFollower) behavior); // Do this action while the lock is held!
    }

    RaftNodeBehaviorFollower convertToFollowerForNewLeader(String remoteNodeId, AppendEntries appendEntries) {
        behavior.close();
        final int oldTerm = behavior.term;
        rollback(oldTerm, appendEntries.getTerm());

        behavior = new RaftNodeBehaviorFollower(this, appendEntries.getTerm());
        ((RaftNodeBehaviorFollower) behavior).setLeaderId(remoteNodeId);
        LOGGER.info("{} Changed leader to {}", id, remoteNodeId);

        return (RaftNodeBehaviorFollower) behavior;
    }

    RaftNodeBehaviorCandidate convertToCandidate(int newTerm) {
        behavior.close();
        // TODO: Does a rollback need to happen at this state?
        // final int oldTerm = behavior.getTerm();
        // rollback(oldTerm, newTerm);

        behavior = new RaftNodeBehaviorCandidate(this, newTerm);
        return (RaftNodeBehaviorCandidate) behavior;
    }

    RaftNodeBehaviorLeader convertToLeader() {
        behavior.close();

        final int term = behavior.term;
        behavior = new RaftNodeBehaviorLeader(this, term);
        return (RaftNodeBehaviorLeader) behavior;
    }

    // endregion

    // region RaftNode interface

    @Override
    public String getId() {
        return id;
    }

    @Override
    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    @Override
    public CompletableFuture<TermIndex> getCurrentTermIndex() {
        return CompletableFuture.supplyAsync(() -> {
            // Return the sample pinned by the most recent getNodeState() read if this
            // node was observed as LEADER at that instant: it is the term/index that
            // BELONGS to that observation, not whatever the live behavior holds by the
            // time this task runs (a conversion may have landed between the two tasks).
            // Consume it so a later standalone read -- which is asked for the LIVE
            // position, e.g. a catch-up or log-length check -- is not served stale data.
            final TermIndex pinned = pendingLeaderTermIndex;
            if (pinned != null) {
                pendingLeaderTermIndex = null;
                return pinned;
            }
            return new TermIndex(behavior.term, logs.getCurrentTermIndex().getIndex());
        }, nodeExecutor);
    }

    @Override
    public CompletableFuture<TermIndex> getCommittedTermIndex() {
        return CompletableFuture.supplyAsync(logs::getCommittedTermIndex, nodeExecutor);
    }

    @Override
    public CompletableFuture<Void> start() {
        return CompletableFuture.runAsync(this::startInternal, nodeExecutor);
    }

    @Override
    public CompletableFuture<Void> stop() {
        return CompletableFuture.runAsync(this::stopInternal, nodeExecutor);
    }

    @Override
    public CompletableFuture<NodeStates> getNodeState() {
        return CompletableFuture.supplyAsync(() -> {
            final NodeStates state = behavior.state;
            // Sample the TermIndex in the SAME executor task as the state, so the pair
            // a reader collects from state + the immediate term read describes one
            // observation. Only a LEADER sample is pinned: for non-leaders the live
            // term read is the correct (and expected) answer for catch-up/bookkeeping,
            // and no assertion derives a term from a non-leader's state.
            pendingLeaderTermIndex = state == NodeStates.LEADER
                    ? new TermIndex(behavior.term, logs.getCurrentTermIndex().getIndex())
                    : null;
            return state;
        }, nodeExecutor);
    }

    @Override
    public CompletableFuture<List<NodeIdentifier>> getKnownNodes() {
        return CompletableFuture.supplyAsync(clusterTopology::getTopology, nodeExecutor);
    }


    @Override
    public CompletableFuture<List<NodeConnectionOutbound>> connectWithTopology() {
        return CompletableFuture.supplyAsync(this::newTopologyConnections, nodeExecutor);
    }

    @Override
    public CompletableFuture<NodeConnectionOutbound> connectTo(NodeAddress nodeAddress) {
        return CompletableFuture.supplyAsync(() -> newConnection(nodeAddress), nodeExecutor);
    }

    @Override
    public CompletableFuture<NodeConnectionOutbound> acceptConnection(ChannelMiddleware.ChannelSide incoming) {
        return CompletableFuture.supplyAsync(() -> onNewConnection(incoming), nodeExecutor);
    }

    @Override
    public CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection) {
        return CompletableFuture.runAsync(() -> disconnectFromInternal(connection), nodeExecutor);
    }

    @Override
    public CompletableFuture<Optional<StateResponse>> getLeader() {
        return getClusterNodeStates()
                .thenApply(states -> states.stream()
                        .filter(x -> x.getState().equals(NodeStates.LEADER))
                        .max(Comparator.comparingInt(StateResponse::getTerm)));
    }

    @Override
    public CompletableFuture<List<StateResponse>> getClusterNodeStates() {
        return CompletableFuture.completedFuture(null)
                .thenComposeAsync(_void -> {
                    final List<CompletableFuture<StateResponse>> futures = activeConnections.stream()
                            .map(c -> c.sendStateRequest(new StateRequest()))
                            .collect(Collectors.toList());

                    return AsyncUtilities.sequence(futures)
                            .thenApplyAsync(stateResponses -> {
                                // Add self into the response list
                                stateResponses.add(behavior.getStateResponse(new StateRequest()));
                                return stateResponses;
                            }, nodeExecutor);
                }, nodeExecutor);
    }

    @Override
    public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
        return CompletableFuture.completedFuture(null)
                .thenComposeAsync(x -> {
                    if (behavior.state != NodeStates.LEADER) {
                        throw new IllegalStateException("Not currently a leader! Instead " + behavior.state);
                    }

                    return logs.appendFutureLog(behavior.term, entry);
                }, nodeExecutor);
    }

    // endregion
}
