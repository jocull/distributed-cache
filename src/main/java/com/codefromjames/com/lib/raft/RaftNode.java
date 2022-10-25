package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.ClusterTopology;
import com.codefromjames.com.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);
    private static final Random RANDOM = new Random();

    // Node details
    private final String id;
    private final NodeAddress nodeAddress;
    private NodeStates state = NodeStates.FOLLOWER; // All nodes start in FOLLOWER state until they hear from a leader or start an election

    // Node management
    private final RaftManager manager;

    // Topology and connections
    private final ClusterTopology clusterTopology;
    private final List<NodeCommunication> activeConnections = new ArrayList<>();

    // Log management
    private final AtomicLong commitIndex = new AtomicLong();
    private final AtomicLong lastReceivedIndex = new AtomicLong();
    // TODO: Need some better structures here?
    //       Ways to manage log compaction?
    //       Ways to make data pending, but not yet committed?
    //       Ways to disconnect the logs themselves from the state machines they manage?

    // Leadership
    private ScheduledFuture<?> heartbeatTimeout;
    private String leaderId;

    // Elections
    private ScheduledFuture<?> electionTimeout;
    private ActiveElection activeElection = null;
    private final AtomicInteger currentTerm = new AtomicInteger();

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
        // TODO: This only starts if we're a follower, otherwise we emit heartbeats on an interval.
        scheduleNextElectionTimeout();
    }

    public String getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public NodeStates getState() {
        return state;
    }

    public long getLastReceivedIndex() {
        return lastReceivedIndex.get();
    }

    public void connectWithTopology() {
        final List<NodeAddress> nodeAddresses = manager.discoverNodes();
        for (NodeAddress remoteAddress : nodeAddresses) {
            // Skip self
            if (remoteAddress.getAddress().equals(getNodeAddress().getAddress())) {
                continue;
            }
            // Skip nodes already connected with
            synchronized (activeConnections) {
                if (hasNodeConnection(remoteAddress)) {
                    LOGGER.debug("{} Already has connection to {} - skipping", id, remoteAddress.getAddress());
                    continue;
                }
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
        final NodeCommunication connection = new NodeCommunication(this, incoming);
        synchronized (activeConnections) {
            activeConnections.add(connection);
        }
        return connection;
    }

    private boolean hasNodeConnection(NodeAddress remoteAddress) {
        synchronized (activeConnections) {
            if (activeConnections.stream().anyMatch(c -> c.getRemoteNodeAddress().equals(remoteAddress))) {
                return true;
            }
            return false;
        }
    }

    public ClusterTopology getClusterTopology() {
        return clusterTopology;
    }

    private void scheduleNextElectionTimeout() {
        if (!NodeStates.LEADER.equals(state)) {
            // The election timeout is randomized to be between 150ms and 300ms.
            if (electionTimeout != null
                    && !electionTimeout.isCancelled()
                    && !electionTimeout.isDone()) {
                electionTimeout.cancel(false);
                electionTimeout = null;
            }
            electionTimeout = manager.schedule(this::onElectionTimeout, 150 + RANDOM.nextInt(151), TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleNextHeartbeat() {
        if (NodeStates.LEADER.equals(state)) {
            if (electionTimeout != null) {
                electionTimeout.cancel(false);
                electionTimeout = null;
            }
            heartbeatTimeout = manager.schedule(this::onHeartbeatTimeout, 50, TimeUnit.MILLISECONDS);
        }
    }

    private void onHeartbeatTimeout() {
        activeConnections.forEach(c -> c.appendEntries(new AppendEntries(currentTerm.get(), 0L, commitIndex.get())));

        // Chain to the next heartbeat
        scheduleNextHeartbeat();
    }

    private void onElectionTimeout() {
        startNextElection();

        // The election timeout keeps going while we wait for responses back.
        electionTimeout = null; // Avoid canceling this process when scheduling next.
        scheduleNextElectionTimeout();
    }

    private synchronized void startNextElection() {
        final int previousTerm = currentTerm.getAndIncrement();
        final int nextTerm = previousTerm + 1;
        final long lastCommittedLogIndex = this.commitIndex.get();
        final VoteRequest voteRequest = new VoteRequest(nextTerm, id, lastCommittedLogIndex, previousTerm);

        // After the election timeout the follower becomes a candidate and starts a new election term...
        leaderId = null;
        state = NodeStates.CANDIDATE;
        LOGGER.info("{} Candidate node starting a new election at term {}", id, nextTerm);

        activeElection = new ActiveElection(nextTerm);
        activeElection.votedForNodeId = id; // ...votes for itself...
        activeElection.voteCount++;

        // ...and sends out Request Vote messages to other nodes.
        activeConnections.forEach(c -> c.requestVote(voteRequest));
    }

    public synchronized void registerVote(String incomingNodeId, VoteResponse vote) {
        if (activeElection == null) {
            LOGGER.warn("{} Received a vote, but no election is active: {}", id, vote);
            return;
        }
        if (vote.getTerm() != currentTerm.get()) {
            LOGGER.warn("{} Received a vote, but for the wrong term: {}", id, vote);
            return;
        }
        if (vote.isVoteGranted()) {
            LOGGER.info("{} Received a vote for term {} from node {}", id, vote.getTerm(), incomingNodeId);
            activeElection.voteCount++;
            final int majority = clusterTopology.getMajorityCount();
            if (activeElection.voteCount >= majority) {
                // This node has won the election.
                // The leader begins sending out Append Entries messages to its followers.
                LOGGER.info("{} Became leader of term {} with {} votes of required majority {}", incomingNodeId, activeElection.term, activeElection.voteCount, majority);
                activeElection = null;
                state = NodeStates.LEADER;
                scheduleNextHeartbeat();
            }
        }
    }

    public synchronized Optional<VoteResponse> requestVote(String requestingNodeId, VoteRequest voteRequest) {
        if (voteRequest.getTerm() < this.currentTerm.get()) {
            LOGGER.warn("{} Received a vote request from {} for a term lower than current term: {} vs {}", id, requestingNodeId, voteRequest.getTerm(), this.currentTerm.get());
            return Optional.empty();
        }
        if (activeElection != null) {
            if (activeElection.term == voteRequest.getTerm()) {
                LOGGER.warn("{} Received a vote request from {} for term {} but already voted for {}", id, requestingNodeId, voteRequest.getTerm(), activeElection.votedForNodeId);
                return Optional.empty();
            }
            LOGGER.info("{} Resetting active election from term {} to {}", id, activeElection.term, voteRequest.getTerm());
        }

        // If the receiving node hasn't voted yet in this term then it votes for the candidate...
        // ...and the node resets its election timeout.
        if (leaderId != null) {
            LOGGER.info("{} Removing current leader {}", id, leaderId);
            leaderId = null; // Remove the current leader
        }

        final boolean grantVote = voteRequest.getLastLogIndex() >= getLastReceivedIndex();
        activeElection = new ActiveElection(voteRequest.getTerm());
        activeElection.votedForNodeId = grantVote ? requestingNodeId : id; // Vote for self instead

        return Optional.of(new VoteResponse(voteRequest.getTerm(), grantVote));
    }

    public void appendEntries(String requestingNodeId, AppendEntries appendEntries) {
        // TODO: Needs to ack each append message
        if (appendEntries.getTerm() < this.currentTerm.get()) {
            LOGGER.warn("{} Received append entries from {} for a term lower than current term: {} vs {}", id, requestingNodeId, appendEntries.getTerm(), this.currentTerm.get());
            return;
        }

        if (leaderId == null) {
            leaderId = requestingNodeId;
            state = NodeStates.FOLLOWER;
            currentTerm.set(appendEntries.getTerm());
            LOGGER.info("{} Made follower of {}", id, requestingNodeId);
        } else if (!leaderId.equals(requestingNodeId)) {
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", id, requestingNodeId, leaderId);
            return; // Does not count as a heartbeat
        }

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
    }

    private static class ActiveElection {
        private final int term;
        private int voteCount;
        private String votedForNodeId;

        public ActiveElection(int term) {
            this.term = term;
        }
    }
}
