package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.ClusterTopology;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    // Node management
    private final RaftManager manager;

    // Node details
    private final String id;
    private final NodeAddress nodeAddress;
    private RaftNodeBehavior behavior = new RaftNodeBehaviorFollowerInitial(this);

    // Topology and connections
    private final ClusterTopology clusterTopology;
    private final List<NodeConnectionOutbound> activeConnections = new ArrayList<>();

    // Log management
    private final RaftLogs logs = new RaftLogs();

    // Node interface
    private final RaftOperations raftOperations = new RaftOperationsImpl(this);

    public RaftNode(String id,
                    NodeAddress nodeAddress,
                    RaftManager manager) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.manager = manager;

        // Each node has its own view of cluster topology and self registers with it
        this.clusterTopology = new ClusterTopology();
        this.clusterTopology.register(new NodeIdentifier(id, nodeAddress));

        // Make the manager aware that this node is referencing it
        this.manager.addManagedNode(this);
    }

    public synchronized void start() {
        // All nodes start in FOLLOWER state until they hear from a leader or start an election
        behavior.close();
        behavior = new RaftNodeBehaviorFollower(this, 0);
    }

    public synchronized void stop() {
        // Stopping the node stops any timers and resets the term back to a beginning state
        behavior.close();
        behavior = new RaftNodeBehaviorFollowerInitial(this);
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public synchronized NodeStates getState() {
        return behavior.state();
    }

    public TermIndex getLastReceivedIndex() {
        return logs.getCurrentTermIndex();
    }

    /* package-private-for-test */ RaftLogs getLogs() {
        return logs;
    }

    public synchronized int getCurrentTerm() {
        return behavior.term();
    }

    public ClusterTopology getClusterTopology() {
        return clusterTopology;
    }

    public RaftManager getManager() {
        return manager;
    }

    public RaftOperations getOperations() {
        return raftOperations;
    }

    /* package-private-for-test */ List<NodeConnectionOutbound> getActiveConnections() {
        synchronized (activeConnections) {
            return List.copyOf(activeConnections);
        }
    }

    private void rollback(int oldTerm, int newTerm) {
        final TermIndex previousIndex = logs.getCurrentTermIndex();
        final List<RaftLog<?>> rollback = logs.rollback();
        final TermIndex newIndex = logs.getCurrentTermIndex();
        LOGGER.info("{} Rolling back {} logs from term {} -> {}, index {} -> {}",
                id, rollback.size(), oldTerm, newTerm, previousIndex, newIndex);
    }

    // region Connection handling

    public void connectWithTopology() {
        final List<NodeAddress> nodeAddresses = manager.discoverNodes();
        for (NodeAddress remoteAddress : nodeAddresses) {
            // Skip self
            if (remoteAddress.getAddress().equals(getNodeAddress().getAddress())) {
                continue;
            }
            // Skip nodes already connected with
            if (hasNodeConnection(remoteAddress)) {
                LOGGER.debug("{} Already has connection to {} - skipping", id, remoteAddress.getAddress());
                continue;
            }
            connectTo(remoteAddress);
        }
    }

    /**
     * When this node makes a connection to another node.
     */
    public NodeConnectionOutbound connectTo(NodeAddress remoteAddress) {
        if (hasNodeConnection(remoteAddress)) {
            throw new IllegalStateException("Node " + remoteAddress.getAddress() + " already has a connection");
        }

        LOGGER.debug("{} Establishing connection to {}", id, remoteAddress.getAddress());
        final NodeConnectionOutbound connection = new NodeConnectionOutboundImpl(() -> behavior, manager.openChannel(this, remoteAddress));
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

    /**
     * When a connection is made to this node.
     */
    public NodeConnectionOutbound onConnection(ChannelMiddleware.ChannelSide incoming) {
        LOGGER.debug("{} Incoming connection from {}", id, incoming.getAddress());
        final NodeConnectionOutbound connection = new NodeConnectionOutboundImpl(() -> behavior, incoming);
        synchronized (activeConnections) {
            activeConnections.add(connection);
        }
        return connection;
    }

    public void disconnect(NodeConnectionOutbound connection) {
        LOGGER.debug("{} Disconnecting from {}", id, connection.getRemoteNodeAddress().getAddress());
        throw new UnsupportedOperationException("Disconnect not yet implemented!");
    }

    private boolean hasNodeConnection(NodeAddress remoteAddress) {
        synchronized (activeConnections) {
            return activeConnections.stream().anyMatch(c -> c.getRemoteNodeAddress().equals(remoteAddress));
        }
    }

    // endregion

    // region Node type conversions

    synchronized <TOut> TOut convertToFollower(int newTerm, Function<RaftNodeBehaviorFollower, TOut> fnActionWithLock) {
        behavior.close();
        final int oldTerm = behavior.term();
        rollback(oldTerm, newTerm);

        behavior = new RaftNodeBehaviorFollower(this, newTerm);
        return fnActionWithLock.apply((RaftNodeBehaviorFollower) behavior); // Do this action while the lock is held!
    }

    synchronized RaftNodeBehaviorFollower convertToFollowerForNewLeader(String remoteNodeId, AppendEntries appendEntries) {
        behavior.close();
        final int oldTerm = behavior.term();
        rollback(oldTerm, appendEntries.getTerm());

        behavior = new RaftNodeBehaviorFollower(this, appendEntries.getTerm());
        ((RaftNodeBehaviorFollower) behavior).setLeaderId(remoteNodeId);
        LOGGER.info("{} Changed leader to {}", id, remoteNodeId);

        return (RaftNodeBehaviorFollower) behavior;
    }

    synchronized RaftNodeBehaviorCandidate convertToCandidate(int newTerm) {
        behavior.close();
        // TODO: Does a rollback need to happen at this state?
        // final int oldTerm = behavior.getTerm();
        // rollback(oldTerm, newTerm);

        behavior = new RaftNodeBehaviorCandidate(this, newTerm);
        return (RaftNodeBehaviorCandidate) behavior;
    }

    synchronized RaftNodeBehaviorLeader convertToLeader() {
        behavior.close();

        final int term = behavior.term();
        behavior = new RaftNodeBehaviorLeader(this, term);
        return (RaftNodeBehaviorLeader) behavior;
    }

    // endregion
}
