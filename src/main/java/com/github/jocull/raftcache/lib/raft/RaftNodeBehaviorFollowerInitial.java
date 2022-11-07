package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

import java.util.Optional;

class RaftNodeBehaviorFollowerInitial extends RaftNodeBehavior {
    public RaftNodeBehaviorFollowerInitial(RaftNode self) {
        super(self, NodeStates.FOLLOWER, 0);
    }

    @Override
    void closeInternal() {
        // no-op
    }

    @Override
    Optional<VoteResponse> onVoteRequest(NodeCommunication remote, VoteRequest voteRequest) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    void onVoteResponse(NodeCommunication remote, VoteResponse voteResponse) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    AcknowledgeEntries onAppendEntries(NodeCommunication remote, AppendEntries appendEntries) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    void onAcknowledgeEntries(NodeCommunication remote, AcknowledgeEntries acknowledgeEntries) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }
}
