package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

abstract class RaftNodeBehavior implements NodeCommunicationReceiver {
    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    final RaftNodeImpl self;
    final NodeStates state;
    final int term;
    private volatile boolean terminated = false;

    public RaftNodeBehavior(RaftNodeImpl self, NodeStates state, int term) {
        this.self = self;
        this.state = state;
        this.term = term;
    }

    boolean isTerminated() {
        return terminated;
    }

    void close() {
        terminated = true;
        closeInternal();
    }

    abstract void closeInternal();

    @Override
    public void onIntroduction(NodeConnectionOutbound sender, Introduction introduction) {
        // Mark the connection with the identifying node
        sender.setRemoteNodeId(introduction.getId());

        // We register data about the node that has introduced itself
        self.clusterTopology.register(new NodeIdentifier(
                introduction.getId(),
                introduction.getNodeAddress()));

        // And reply with the cluster topology as we know it
        final AnnounceClusterTopology response = new AnnounceClusterTopology(
                self.clusterTopology.getTopology().stream()
                        .map(i -> new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(
                                i.getId(),
                                i.getNodeAddress()
                        ))
                        .collect(Collectors.toList()));

        sender.sendAnnounceClusterTopology(response);
    }

    @Override
    public void onAnnounceClusterTopology(NodeConnectionOutbound sender, AnnounceClusterTopology announceClusterTopology) {
        // When the topology has been received we can update our local view of the world
        self.clusterTopology.register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifier(
                        i.getId(),
                        i.getNodeAddress()
                ))
                .collect(Collectors.toList()));

        if (sender.getRemoteNodeId() == null) {
            final String remoteNodeId = self.clusterTopology.locate(sender.getRemoteNodeAddress())
                    .map(com.github.jocull.raftcache.lib.topology.NodeIdentifier::getId)
                    .orElseThrow(() -> new IllegalStateException("Remote address " + sender.getRemoteNodeAddress() + " not found in cluster topology: {}" + self.clusterTopology.getTopology()));
            sender.setRemoteNodeId(remoteNodeId);
        }
    }

    /**
     * Raft §5.4.2 up-to-date rule evaluated against a candidate's last log entry from a
     * Pre-Vote request: grant iff the candidate's last entry is at least as up-to-date as
     * our own (higher term, or equal term and higher-or-equal index).
     *
     * <p>Pure read: the comparison is made against our CURRENT, unmutated log — exactly as
     * the vote-grant path requires (see {@code convertToFollowerForVote}). The term/index
     * comparison deliberately excludes any state mutation; pre-vote handling must never
     * change term, {@code votedFor}, or timers.
     */
    boolean isUpToDate(PreVoteRequest preVoteRequest) {
        final TermIndex incomingTermIndex = new TermIndex(
                preVoteRequest.getLastLogTermIndex().getTerm(),
                preVoteRequest.getLastLogTermIndex().getIndex());
        final TermIndex currentIndex = self.logs.getCurrentTermIndex();
        LOGGER.trace("{} Pre-vote up-to-date check: candidate {} vs own log {} -> {}",
                self.getId(), incomingTermIndex, currentIndex, incomingTermIndex.compareTo(currentIndex) >= 0);
        return incomingTermIndex.compareTo(currentIndex) >= 0;
    }

    StateResponse getStateResponse(StateRequest request) {
        final TermIndex current = self.logs.getCurrentTermIndex();
        final TermIndex committed = self.logs.getCommittedTermIndex();
        return new StateResponse(
                request,
                self.getId(),
                self.getNodeAddress(),
                state,
                term,
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        current.getTerm(),
                        current.getIndex()),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        committed.getTerm(),
                        committed.getIndex()));
    }

    @Override
    public void onStateRequest(NodeConnectionOutbound sender, StateRequest stateRequest) {
        final StateResponse response =getStateResponse(stateRequest);
        sender.sendStateResponse(response);
    }
}
