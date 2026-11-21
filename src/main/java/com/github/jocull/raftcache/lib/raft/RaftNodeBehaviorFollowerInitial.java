package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

class RaftNodeBehaviorFollowerInitial extends RaftNodeBehavior {
    public RaftNodeBehaviorFollowerInitial(RaftNodeImpl self) {
        super(self, NodeStates.FOLLOWER, 0);
    }

    @Override
    void closeInternal() {
        // no-op
    }

    @Override
    public void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    public void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    public void onPreVoteRequest(NodeConnectionOutbound sender, PreVoteRequest preVoteRequest) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    public void onPreVoteResponse(NodeConnectionOutbound sender, PreVoteResponse preVoteResponse) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    public void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }

    @Override
    public void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries) {
        throw new IllegalStateException(self.getId() + ": Initial follower is not yet started.");
    }
}
