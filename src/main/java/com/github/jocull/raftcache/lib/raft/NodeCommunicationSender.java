package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;

public interface NodeCommunicationSender {
    void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology);

    void sendStateResponse(StateResponse stateResponse);

    void sendVoteResponse(VoteResponse voteResponse);

    void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries);
}
