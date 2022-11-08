package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;

public interface NodeCommunicationReceiver {
    void onIntroduction(NodeConnectionOutbound sender, Introduction introduction);

    void onAnnounceClusterTopology(NodeConnectionOutbound sender, AnnounceClusterTopology announceClusterTopology);

    void onStateRequest(NodeConnectionOutbound sender, StateRequest stateRequest);

    void onStateResponse(NodeConnectionOutbound sender, StateResponse stateResponse);

    void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest);

    void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse);

    void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries);

    void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries);
}
