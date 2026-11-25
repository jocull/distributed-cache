package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;

public interface NodeCommunicationReceiver {
    void onIntroduction(NodeConnectionOutbound sender, Introduction introduction);

    void onAnnounceClusterTopology(NodeConnectionOutbound sender, AnnounceClusterTopology announceClusterTopology);

    void onStateRequest(NodeConnectionOutbound sender, StateRequest stateRequest);

    void onVoteRequest(NodeConnectionOutbound sender, VoteRequest voteRequest);

    void onVoteResponse(NodeConnectionOutbound sender, VoteResponse voteResponse);

    void onPreVoteRequest(NodeConnectionOutbound sender, PreVoteRequest preVoteRequest);

    void onPreVoteResponse(NodeConnectionOutbound sender, PreVoteResponse preVoteResponse);

    void onAppendEntries(NodeConnectionOutbound sender, AppendEntries appendEntries);

    void onAcknowledgeEntries(NodeConnectionOutbound sender, AcknowledgeEntries acknowledgeEntries);

    void onInstallSnapshot(NodeConnectionOutbound sender, InstallSnapshot installSnapshot);

    void onInstallSnapshotResponse(NodeConnectionOutbound sender, InstallSnapshotResponse installSnapshotResponse);

    // Client B §3.3 (2026-11-25): the client verb handlers (pre-introduction dispatch,
    // B §4.1 — these never require remoteNodeId).
    void onCachePut(NodeConnectionOutbound sender, CachePutRequest request);

    void onCacheDelete(NodeConnectionOutbound sender, CacheDeleteRequest request);

    void onCacheExpire(NodeConnectionOutbound sender, CacheExpireRequest request);

    void onCacheRead(NodeConnectionOutbound sender, CacheReadRequest request);
}
