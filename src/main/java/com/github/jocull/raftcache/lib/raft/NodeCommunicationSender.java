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

    // Client B §3.3 (2026-11-25): the client verb set — correlated sends (the request
    // future is completed by the matching reply frame) plus the void reply senders.
    CompletableFuture<CacheWriteResponse> sendCachePut(CachePutRequest request);

    CompletableFuture<CacheWriteResponse> sendCacheDelete(CacheDeleteRequest request);

    CompletableFuture<CacheWriteResponse> sendCacheExpire(CacheExpireRequest request);

    CompletableFuture<CacheReadResponse> sendCacheRead(CacheReadRequest request);

    void sendCacheWriteResponse(CacheWriteResponse response);

    void sendCacheReadResponse(CacheReadResponse response);
}
