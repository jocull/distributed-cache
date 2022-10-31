package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AcknowledgeEntries;
import com.codefromjames.com.lib.raft.messages.AppendEntries;
import com.codefromjames.com.lib.raft.messages.VoteRequest;
import com.codefromjames.com.lib.raft.messages.VoteResponse;

import java.util.Optional;

class RaftNodeBehaviorFollowerInitial extends RaftNodeBehavior {
    public RaftNodeBehaviorFollowerInitial(RaftNode self) {
        super(self, NodeStates.FOLLOWER, 0);
    }

    @Override
    void close() {
        // no-op
    }

    @Override
    Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        throw new IllegalStateException("Initial follower is not yet started.");
    }

    @Override
    void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        throw new IllegalStateException("Initial follower is not yet started.");
    }

    @Override
    AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        throw new IllegalStateException("Initial follower is not yet started.");
    }

    @Override
    void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        throw new IllegalStateException("Initial follower is not yet started.");
    }
}
