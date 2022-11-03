package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.ClusterTopology;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.raft.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
    private final List<NodeCommunication> activeConnections = new ArrayList<>();

    // Log management
    private final RaftLogs logs = new RaftLogs();

    // Node interface
    private final RaftOperations raftOperations = new LocalRaftOperations();

    public RaftNode(String id,
                    NodeAddress nodeAddress,
                    RaftManager manager) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.manager = manager;

        // Each node has its own view of cluster topology
        clusterTopology = new ClusterTopology();

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
        return behavior.getState();
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

    public synchronized int getCurrentTerm() {
        return behavior.getTerm();
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

    /* package-private-for-test */ List<NodeCommunication> getActiveConnections() {
        synchronized (activeConnections) {
            return List.copyOf(activeConnections);
        }
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

    public void disconnect(NodeCommunication connection) {
        LOGGER.debug("{} Disconnecting from {}", id, connection.getRemoteNodeAddress().getAddress());
        throw new UnsupportedOperationException("Disconnect not yet implemented!");
//        synchronized (activeConnections) {
//            if (!activeConnections.removeIf(c -> c.getRemoteNodeAddress().equals(connection.getRemoteNodeAddress()))) {
//                throw new IllegalArgumentException("There is no active connection to " + connection.getRemoteNodeAddress().getAddress());
//            }
//        }
    }

    private boolean hasNodeConnection(NodeAddress remoteAddress) {
        synchronized (activeConnections) {
            if (activeConnections.stream().anyMatch(c -> c.getRemoteNodeAddress().equals(remoteAddress))) {
                return true;
            }
            return false;
        }
    }

    private void rollback(int oldTerm, int newTerm) {
        final long previousIndex = logs.getCurrentIndex();
        final List<RaftLog<?>> rollback = logs.rollback();
        final long newIndex = logs.getCurrentIndex();
        LOGGER.info("{} Rolling back {} logs from term {} -> {}, index {} -> {}",
                id, rollback.size(), oldTerm, newTerm, previousIndex, newIndex);
    }

    // region Node type conversions

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

        final int term = behavior.getTerm();
        behavior = new RaftNodeBehaviorLeader(this, term);
        return (RaftNodeBehaviorLeader) behavior;
    }

    // endregion

    // region Behavior delegate methods

    synchronized AnnounceClusterTopology onIntroduction(Introduction introduction) {
        return behavior.onIntroduction(introduction);
    }

    synchronized void onAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        behavior.onAnnounceClusterTopology(announceClusterTopology);
    }

    synchronized Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        return behavior.onVoteRequest(remote, voteRequest);
    }

    synchronized void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        behavior.onVoteResponse(remote, voteResponse);
    }

    synchronized AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        return behavior.onAppendEntries(remote, appendEntries);
    }

    synchronized void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        behavior.onAcknowledgeEntries(remote, acknowledgeEntries);
    }

    // endregion

    private class LocalRaftOperations implements RaftOperations {
        @Override
        public <T> RaftLog<T> submitNewLog(T entry) {
            if (getState() != NodeStates.LEADER) {
                throw new IllegalStateException("Not currently a leader! Instead " + getState());
            }

            return logs.appendLog(getCurrentTerm(), entry);
        }

        @Override
        public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
            if (getState() != NodeStates.LEADER) {
                throw new IllegalStateException("Not currently a leader! Instead " + getState());
            }

            return logs.appendFutureLog(getCurrentTerm(), entry);
        }
    }
}
