package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.ClusterTopology;
import com.codefromjames.com.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    // Node details
    private final String id;
    private final NodeAddress nodeAddress;
    private volatile NodeStates state = NodeStates.FOLLOWER; // All nodes start in FOLLOWER state until they hear from a leader or start an election

    // Node management
    private final RaftManager manager;

    // Topology and connections
    private final ClusterTopology clusterTopology;
    private final List<NodeCommunication> activeConnections = new ArrayList<>();

    // Log management
    private final RaftLogs logs = new RaftLogs();

    // Leadership
    private volatile ScheduledFuture<?> heartbeatTimeout;
    private volatile String leaderId;

    // Elections
    private volatile ScheduledFuture<?> electionTimeout;
    private volatile ActiveElection activeElection = null;
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

    // TODO: Clean up, part of the client API with the RaftNode
    public <T> RaftLog<T> submitNewLog(T entry) {
        if (state != NodeStates.LEADER) {
            throw new IllegalStateException("Not currently a leader! Instead " + state);
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
        return currentTerm.get();
    }

    public ClusterTopology getClusterTopology() {
        return clusterTopology;
    }

    public RaftManager getManager() {
        return manager;
    }

    private void scheduleNextElectionTimeout() {
        if (!NodeStates.LEADER.equals(state)) {
            if (electionTimeout != null
                    && !electionTimeout.isCancelled()
                    && !electionTimeout.isDone()) {
                electionTimeout.cancel(false);
            }
            // The election timeout is randomized to be between 150ms and 300ms.
            electionTimeout = manager.schedule(this::onElectionTimeout, 150 + RaftManager.RANDOM.nextInt(151), TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleNextHeartbeat() {
        if (NodeStates.LEADER.equals(state)) {
            if (electionTimeout != null) {
                electionTimeout.cancel(false);
                electionTimeout = null;
            }
            LOGGER.debug("{} Scheduled next heartbeat", id);
            if (heartbeatTimeout != null
                    && !heartbeatTimeout.isDone()
                    && !heartbeatTimeout.isCancelled()) {
                heartbeatTimeout.cancel(false);
            }
            heartbeatTimeout = manager.schedule(() -> this.onHeartbeatTimeout(false), 50, TimeUnit.MILLISECONDS);
        }
    }

    private static AppendEntries.RaftLog transformLog(RaftLog<?> r) {
        return new AppendEntries.RaftLog(r.getIndex(), r.getEntry());
    }

    private void onHeartbeatTimeout(boolean forced) {
        if (forced) {
            LOGGER.debug("{} Forced heartbeat timeout!", id);
        } else {
            LOGGER.debug("{} Heartbeat timeout!", id);
        }

        synchronized (activeConnections) {
            activeConnections.forEach(c -> {
                final List<AppendEntries.RaftLog> entries = logs.getLogRange(c.getCurrentIndex(), 25, RaftNode::transformLog);
                if (!entries.isEmpty()) {
                    final long newEndIndex = entries.get(entries.size() - 1).getIndex();
                    LOGGER.debug("{} Appending entries to {} setting index {} -> {} with {} entries", id, c.getRemoteNodeId().orElseThrow(), c.getCurrentIndex(), newEndIndex, entries.size());
                } else {
                    LOGGER.debug("{} Sending heartbeat to {}", id, c.getRemoteNodeId().orElseThrow());
                }
                c.appendEntries(new AppendEntries(
                        currentTerm.get(),
                        c.getCurrentIndex(),
                        logs.getCommitIndex(),
                        entries));
            });
        }

        // Chain to the next heartbeat
        heartbeatTimeout = null;
        scheduleNextHeartbeat();
    }

    private synchronized void onElectionTimeout() {
        // After the election timeout the follower becomes a candidate and starts a new election term...
        // ...and sends out Request Vote messages to other nodes.
        final int previousTerm = currentTerm.getAndIncrement();
        final int nextTerm = previousTerm + 1;
        final long lastCommittedLogIndex = logs.getCommitIndex();
        final VoteRequest voteRequest = new VoteRequest(nextTerm, id, lastCommittedLogIndex, previousTerm);
        leaderId = null;
        state = NodeStates.CANDIDATE;
        LOGGER.info("{} Candidate starting a new election at term {}", id, nextTerm);
        activeElection = new ActiveElection(nextTerm);
        activeElection.votedForNodeId = id; // ...votes for itself...
        activeElection.voteCount++;
        synchronized (activeConnections) {
            activeConnections.forEach(c -> c.requestVote(voteRequest));
        }

        // The election timeout keeps going while we wait for responses back.
        electionTimeout = null; // Avoid canceling this process when scheduling next.
        scheduleNextElectionTimeout();
    }

    public synchronized void registerVote(String incomingNodeId, VoteResponse vote) {
        if (activeElection == null) {
            LOGGER.warn("{} Received a vote from {}, but no election is active: {}", id, incomingNodeId, vote);
            return;
        }
        if (vote.getTerm() != currentTerm.get()) {
            LOGGER.warn("{} Received a vote from {}, but for the wrong term: {}", id, incomingNodeId, vote);
            return;
        }
        if (vote.isVoteGranted()) {
            activeElection.voteCount++;
            LOGGER.info("{} Received a vote from {} for term {} and now has {} votes", id, incomingNodeId, vote.getTerm(), activeElection.voteCount);
            final int majority = clusterTopology.getMajorityCount();
            if (activeElection.voteCount >= majority) {
                // This node has won the election.
                // The leader begins sending out Append Entries messages to its followers.
                LOGGER.info("{} Became leader of term {} with {} votes of required majority {} of {}", id, activeElection.term, activeElection.voteCount, majority, clusterTopology.getTopology().size());
                activeElection = null;
                leaderId = id;
                state = NodeStates.LEADER;
                scheduleNextHeartbeat(); // TODO: This is a hack to clear election timeout...
                onHeartbeatTimeout(true); // Send this heartbeat immediately to announce election results
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
        LOGGER.info("{} Voting for {} w/ grant {}", id, activeElection.votedForNodeId, grantVote);
        // Voting resets the election timeout to let the voting process settle
        scheduleNextElectionTimeout();
        return Optional.of(new VoteResponse(voteRequest.getTerm(), grantVote));
    }

    public AcknowledgeEntries appendEntries(String requestingNodeId, AppendEntries appendEntries) {
        if (appendEntries.getTerm() < currentTerm.get()) {
            LOGGER.warn("{} Received append entries from {} for a term lower than current term: {} vs {}", id, requestingNodeId, appendEntries.getTerm(), currentTerm.get());
            return new AcknowledgeEntries(currentTerm.get(), false, logs.getCurrentIndex());
        }
        if (!logs.containsStartPoint(appendEntries.getTerm(), appendEntries.getPreviousLogIndex())) {
            // TODO: A chance to get stuck here? What happens if indexes get out of sync?
            //       How should we reset? Will the election timeout take care of it?
            LOGGER.warn("{} Received append entries from {} term {} with invalid index: {} vs {}", id, requestingNodeId, appendEntries.getTerm(), appendEntries.getPreviousLogIndex(), logs.getCurrentIndex());
            return new AcknowledgeEntries(currentTerm.get(), false, logs.getCurrentIndex());
        }
        if (leaderId != null && !leaderId.equals(requestingNodeId)) {
            LOGGER.warn("{} Append entries request from {} who is not known leader {}", id, requestingNodeId, leaderId);
            return new AcknowledgeEntries(currentTerm.get(), false, logs.getCurrentIndex());
        }
        if (leaderId == null) {
            leaderId = requestingNodeId;
            state = NodeStates.FOLLOWER;
            currentTerm.set(appendEntries.getTerm());
            LOGGER.info("{} Made follower of {}", id, requestingNodeId);
        }

        // Append the logs
        if (!appendEntries.getEntries().isEmpty()) {
            final long newEndIndex = appendEntries.getEntries().get(appendEntries.getEntries().size() - 1).getIndex();
            LOGGER.debug("{} Received entries from {} for index {} -> {} with {} entries", id, requestingNodeId, logs.getCurrentIndex(), newEndIndex, appendEntries.getEntries().size());
        } else {
            LOGGER.debug("{} Received heartbeat from {}", id, requestingNodeId);
        }
        appendEntries.getEntries().forEach(r -> logs.appendLog(appendEntries.getTerm(), r.getIndex(), r.getEntry()));
        // Align the commit index with the leader
        logs.commit(appendEntries.getLeaderCommitIndex());

        // Clear the current timeout and register the next one
        scheduleNextElectionTimeout();
        return new AcknowledgeEntries(currentTerm.get(), true, logs.getCurrentIndex());
    }

    public void notifyTermChange(String remoteNodeId, int term) {
        LOGGER.info("{} Notified of term change from {} to {} by {}", id, currentTerm.get(), term, remoteNodeId);
        if (heartbeatTimeout != null) {
            heartbeatTimeout.cancel(false);
            heartbeatTimeout = null;
        }

        // Reset the follower details
        leaderId = null;
        state = NodeStates.FOLLOWER;
        currentTerm.set(term);

        // Rollback any uncommitted logs
        logs.rollback();

        scheduleNextElectionTimeout();
    }

    public void updateCommittedIndex() {
        final List<Long> currentIndices;
        synchronized (activeConnections) {
            currentIndices = activeConnections.stream()
                    .map(NodeCommunication::getCurrentIndex)
                    .sorted(((Comparator<Long>) Long::compare).reversed())
                    .collect(Collectors.toList());
        }
        final int majorityCount = clusterTopology.getMajorityCount();
        if (currentIndices.size() >= majorityCount) {
            final long majorityMinimumIndex = currentIndices.get(majorityCount - 1);
            if (majorityMinimumIndex > logs.getCommitIndex()) {
                LOGGER.debug("{} Has minimum majority index {} to commit", id, majorityMinimumIndex);
                logs.commit(majorityMinimumIndex);
            }
        } else {
            LOGGER.debug("{} Not a majority to commit with {}", id, currentIndices);
        }
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
