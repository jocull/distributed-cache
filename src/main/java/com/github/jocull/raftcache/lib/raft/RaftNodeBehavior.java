package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

abstract class RaftNodeBehavior implements NodeCommunicationReceiver {
    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final RaftNode self;
    private final NodeStates state;
    private final int term;
    private volatile boolean terminated = false;

    public RaftNodeBehavior(RaftNode self, NodeStates state, int term) {
        this.self = self;
        this.state = state;
        this.term = term;
    }

    RaftNode self() {
        return self;
    }

    NodeStates state() {
        return state;
    }

    int term() {
        return term;
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
        self().getClusterTopology().register(new NodeIdentifier(
                introduction.getId(),
                introduction.getNodeAddress()));

        // And reply with the cluster topology as we know it
        final AnnounceClusterTopology response = new AnnounceClusterTopology(
                self().getClusterTopology().getTopology().stream()
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
        self().getClusterTopology().register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifier(
                        i.getId(),
                        i.getNodeAddress()
                ))
                .collect(Collectors.toList()));

        if (sender.getRemoteNodeId() == null) {
            final String remoteNodeId = self().getClusterTopology().locate(sender.getRemoteNodeAddress())
                    .map(com.github.jocull.raftcache.lib.topology.NodeIdentifier::getId)
                    .orElseThrow(() -> new IllegalStateException("Remote address " + sender.getRemoteNodeAddress() + " not found in cluster topology: {}" + self().getClusterTopology().getTopology()));
            sender.setRemoteNodeId(remoteNodeId);
        }
    }

    @Override
    public void onStateRequest(NodeConnectionOutbound sender, StateRequest stateRequest) {
        final TermIndex current = self().getLogs().getCurrentTermIndex();
        final TermIndex committed = self().getLogs().getCommittedTermIndex();
        final StateResponse response = new StateResponse(
                stateRequest,
                self().getId(),
                self().getNodeAddress(),
                state(),
                term(),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        current.getTerm(),
                        current.getIndex()),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        committed.getTerm(),
                        committed.getIndex()));

        sender.sendStateResponse(response);
    }

    @Override
    public void onStateResponse(NodeConnectionOutbound sender, StateResponse stateResponse) {
        // TODO: Migrate request completion to here? Does this belong here at all?
        throw new UnsupportedOperationException("Not yet finished");
    }
}
