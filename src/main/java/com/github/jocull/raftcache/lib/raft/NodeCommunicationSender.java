package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;

public interface NodeCommunicationSender {
    void sendIntroduction(Introduction introduction);

    void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology);

    void sendStateResponse(StateResponse stateResponse);

    void sendVoteRequest(VoteRequest voteRequest);

    void sendVoteResponse(VoteResponse voteResponse);

    void sendAppendEntries(AppendEntries appendEntries);

    void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries);
}
