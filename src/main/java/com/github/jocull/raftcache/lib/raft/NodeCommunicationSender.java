package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;

import java.util.concurrent.CompletableFuture;

public interface NodeCommunicationSender {
    void sendIntroduction(Introduction introduction);

    void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology);

    CompletableFuture<StateResponse> sendStateRequest(StateRequest stateRequest);

    void sendStateResponse(StateResponse stateResponse);

    void sendVoteRequest(VoteRequest voteRequest);

    void sendVoteResponse(VoteResponse voteResponse);

    void sendPreVoteRequest(PreVoteRequest preVoteRequest);

    void sendPreVoteResponse(PreVoteResponse preVoteResponse);

    void sendAppendEntries(AppendEntries appendEntries);

    void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries);

    void sendInstallSnapshot(InstallSnapshot installSnapshot);

    void sendInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse);
}
