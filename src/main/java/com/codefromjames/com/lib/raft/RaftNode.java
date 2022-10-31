package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.ClusterTopology;
import com.codefromjames.com.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    // Node management
    private final RaftManager manager;

    // Node details
    private final String id;
    private final NodeAddress nodeAddress;
    private volatile RaftNodeBehavior behavior = new RaftNodeBehaviorFollowerInitial(this);

    // Topology and connections
    private final ClusterTopology clusterTopology;
    private final List<NodeCommunication> activeConnections = new ArrayList<>();

    // Log management
    private final RaftLogs logs = new RaftLogs();

    public RaftNode(String id,
                    NodeAddress nodeAddress,
                    RaftManager manager) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.manager = manager;

        // Each node has its own view of cluster topology
        clusterTopology = new ClusterTopology();
    }

    public void start() {
        // All nodes start in FOLLOWER state until they hear from a leader or start an election
        behavior.close();
        behavior = new RaftNodeBehaviorFollower(this, 0);
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public NodeStates getState() {
        return behavior.getState();
    }

    public RaftNodeBehavior getBehavior() {
        return behavior;
    }

    public long getLastReceivedIndex() {
        return logs.getCurrentIndex();
    }

    /* package-private-for-test */ RaftLogs getLogs() {
        return logs;
    }

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
    public NodeCommunication connectTo(NodeAddress remoteAddress) {
        if (hasNodeConnection(remoteAddress)) {
            throw new IllegalStateException("Node " + remoteAddress.getAddress() + " already has a connection");
        }

        LOGGER.debug("{} Establishing connection to {}", id, remoteAddress.getAddress());
        final NodeCommunication connection = new NodeCommunication(this, manager.openChannel(this, remoteAddress));
        synchronized (activeConnections) {
            activeConnections.add(connection);
        }
        connection.introduce();
        return connection;
    }

    /**
     * When a connection is made to this node.
     */
    public NodeCommunication onConnection(ChannelMiddleware.ChannelSide incoming) {
        LOGGER.debug("{} Incoming connection from {}", id, incoming.getAddress());
        final NodeCommunication connection = new NodeCommunication(this, incoming);
        synchronized (activeConnections) {
            activeConnections.add(connection);
        }
        return connection;
    }

    /* package-private-for-test */ List<NodeCommunication> getActiveConnections() {
        synchronized (activeConnections) {
            return List.copyOf(activeConnections);
        }
    }

    public void disconnect(NodeCommunication connection) {
        LOGGER.debug("{} Disconnecting from {}", id, connection.getRemoteNodeAddress().getAddress());
        throw new UnsupportedOperationException("Disconnect not yet implemented!");
//        synchronized (activeConnections) {
//            if (!activeConnections.removeIf(c -> c.getRemoteNodeAddress().equals(connection.getRemoteNodeAddress()))) {
//                throw new IllegalArgumentException("There is no active connection to " + connection.getRemoteNodeAddress().getAddress());
//            }
//        }
    }

    // TODO: Clean up, part of the client API with the RaftNode
    public <T> RaftLog<T> submitNewLog(T entry) {
        if (getState() != NodeStates.LEADER) {
            throw new IllegalStateException("Not currently a leader! Instead " + getState());
        }

        return logs.appendLog(getCurrentTerm(), entry);
    }

    private boolean hasNodeConnection(NodeAddress remoteAddress) {
        synchronized (activeConnections) {
            if (activeConnections.stream().anyMatch(c -> c.getRemoteNodeAddress().equals(remoteAddress))) {
                return true;
            }
            return false;
        }
    }

    public int getCurrentTerm() {
        return behavior.getTerm();
    }

    public ClusterTopology getClusterTopology() {
        return clusterTopology;
    }

    public RaftManager getManager() {
        return manager;
    }

    private void rollback(int oldTerm, int newTerm) {
        final long previousIndex = logs.getCurrentIndex();
        final List<RaftLog<?>> rollback = logs.rollback();
        final long newIndex = logs.getCurrentIndex();
        LOGGER.info("{} Rolling back {} logs from term {} -> {}, index {} -> {}",
                id, rollback.size(), oldTerm, newTerm, previousIndex, newIndex);
    }

    synchronized RaftNodeBehaviorFollower convertToFollower(int newTerm) {
        behavior.close();
        final int oldTerm = behavior.getTerm();
        rollback(oldTerm, newTerm);

        behavior = new RaftNodeBehaviorFollower(this, newTerm);
        return (RaftNodeBehaviorFollower) behavior;
    }

    synchronized RaftNodeBehaviorFollower convertToFollowerForNewLeader(String remoteNodeId, AppendEntries appendEntries) {
        behavior.close();
        final int oldTerm = behavior.getTerm();
        rollback(oldTerm, appendEntries.getTerm());

        behavior = new RaftNodeBehaviorFollower(this, appendEntries.getTerm());
        behavior.setLeaderId(remoteNodeId);
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

        final int term = behavior.getTerm();
        behavior = new RaftNodeBehaviorLeader(this, term);
        return (RaftNodeBehaviorLeader) behavior;
    }
}
