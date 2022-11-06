package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

abstract class RaftNodeBehavior {
    protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    protected final RaftNode self;
    protected final NodeStates state;
    protected final int term;

    public RaftNodeBehavior(RaftNode self, NodeStates state, int term) {
        this.self = self;
        this.state = state;
        this.term = term;
    }

    public NodeStates getState() {
        return state;
    }

    public int getTerm() {
        return term;
    }

    abstract void close();

    AnnounceClusterTopology onIntroduction(Introduction introduction) {
        // We register data about the node that has introduced itself
        self.getClusterTopology().register(new NodeIdentifier(
                introduction.getId(),
                introduction.getNodeAddress()));

        // And reply with the cluster topology as we know it
        return new AnnounceClusterTopology(
                self.getClusterTopology().getTopology().stream()
                        .map(i -> new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(
                                i.getId(),
                                i.getNodeAddress()
                        ))
                        .collect(Collectors.toList()));
    }

    void onAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        // When the topology has been received we can update our local view of the world
        self.getClusterTopology().register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifier(
                        i.getId(),
                        i.getNodeAddress()
                ))
                .collect(Collectors.toList()));
    }

    StateResponse onStateRequest(StateRequest stateRequest) {
        final TermIndex current = self.getLogs().getCurrentTermIndex();
        final TermIndex committed = self.getLogs().getCommittedTermIndex();
        return new StateResponse(
                stateRequest,
                self.getId(),
                self.getNodeAddress(),
                getState(),
                getTerm(),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        current.getTerm(),
                        current.getIndex()),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        committed.getTerm(),
                        committed.getIndex()));
    }

    abstract Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest);

    abstract void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse);

    abstract AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries);

    abstract void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries);
}
