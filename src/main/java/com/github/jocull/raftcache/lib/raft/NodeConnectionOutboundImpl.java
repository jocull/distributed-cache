package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

class NodeConnectionOutboundImpl implements NodeConnectionOutbound {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeConnectionOutboundImpl.class);

    private String remoteNodeId;
    private TermIndex currentTermIndex = new TermIndex(0, 0L);
    private Instant lastEntriesSent = Instant.EPOCH;
    private Instant lastEntriesAcknowledged = Instant.EPOCH;

    private final NodeCommunicationReceiverProvider receiverProvider;
    private final ChannelMiddleware.ChannelSide channel;
    private final NodeRequests requests = new NodeRequests();

    public NodeConnectionOutboundImpl(NodeCommunicationReceiverProvider receiverProvider,
                                      ChannelMiddleware.ChannelSide channel) {
        this.receiverProvider = receiverProvider;
        this.channel = channel;

        this.channel.setReceiver(this::receive);
    }

    @Override
    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    @Override
    public void setRemoteNodeId(String remoteNodeId) {
        this.remoteNodeId = remoteNodeId;
    }

    @Override
    public NodeAddress getRemoteNodeAddress() {
        return channel.getAddress();
    }

    @Override
    public TermIndex getTermIndex() {
        return currentTermIndex;
    }

    @Override
    public void setTermIndex(TermIndex termIndex) {
        currentTermIndex = termIndex;
    }

    @Override
    public Instant getLastEntriesSent() {
        return lastEntriesSent;
    }

    @Override
    public void setLastEntriesSent(Instant lastEntriesSent) {
        this.lastEntriesSent = lastEntriesSent;
    }

    @Override
    public Instant getLastEntriesAcknowledged() {
        return lastEntriesAcknowledged;
    }

    @Override
    public void setLastEntriesAcknowledged(Instant lastEntriesAcknowledged) {
        this.lastEntriesAcknowledged = lastEntriesAcknowledged;
    }

    // TODO: Initial lazy version, not very maintainable with growing number of message types
    private void receive(Object message) {
        // Introductions must be completed first to establish node IDs
        if (message instanceof Introduction) {
            receiverProvider.run(receiver -> receiver.onIntroduction(this, (Introduction) message));
            return;
        }
        if (message instanceof AnnounceClusterTopology) {
            receiverProvider.run(receiver -> receiver.onAnnounceClusterTopology(this, (AnnounceClusterTopology) message));
            return;
        }
        if (message instanceof StateRequest) {
            receiverProvider.run(receiver -> receiver.onStateRequest(this, (StateRequest) message));
            return;
        }
        if (message instanceof StateResponse) {
            receiverProvider.run(receiver -> requests.completeRequest((StateResponse) message));
            return;
        }
        // Client B §4.1 (2026-11-25): client verbs are pre-introduction traffic — a
        // client never performs Introduction, so these must dispatch (like StateRequest)
        // BEFORE the remoteNodeId guard below, not after it.
        if (message instanceof CachePutRequest) {
            receiverProvider.run(receiver -> receiver.onCachePut(this, (CachePutRequest) message));
            return;
        }
        if (message instanceof CacheDeleteRequest) {
            receiverProvider.run(receiver -> receiver.onCacheDelete(this, (CacheDeleteRequest) message));
            return;
        }
        if (message instanceof CacheExpireRequest) {
            receiverProvider.run(receiver -> receiver.onCacheExpire(this, (CacheExpireRequest) message));
            return;
        }
        if (message instanceof CacheReadRequest) {
            receiverProvider.run(receiver -> receiver.onCacheRead(this, (CacheReadRequest) message));
            return;
        }
        if (message instanceof CacheWriteResponse) {
            receiverProvider.run(receiver -> requests.completeRequest((CacheWriteResponse) message));
            return;
        }
        if (message instanceof CacheReadResponse) {
            receiverProvider.run(receiver -> requests.completeRequest((CacheReadResponse) message));
            return;
        }
        if (remoteNodeId == null) {
            throw new IllegalStateException("Cannot process " + message.getClass().getSimpleName()
                    + " before introduction! remoteNodeId is null");
        }

        // After introductions, any message can process
        if (message instanceof VoteRequest) {
            receiverProvider.run(receiver -> receiver.onVoteRequest(this, (VoteRequest) message));
            return;
        }
        if (message instanceof VoteResponse) {
            receiverProvider.run(receiver -> receiver.onVoteResponse(this, (VoteResponse) message));
            return;
        }
        if (message instanceof PreVoteRequest) {
            receiverProvider.run(receiver -> receiver.onPreVoteRequest(this, (PreVoteRequest) message));
            return;
        }
        if (message instanceof PreVoteResponse) {
            receiverProvider.run(receiver -> receiver.onPreVoteResponse(this, (PreVoteResponse) message));
            return;
        }
        if (message instanceof AppendEntries) {
            receiverProvider.run(receiver -> receiver.onAppendEntries(this, (AppendEntries) message));
            return;
        }
        if (message instanceof AcknowledgeEntries) {
            receiverProvider.run(receiver -> receiver.onAcknowledgeEntries(this, (AcknowledgeEntries) message));
            return;
        }
        if (message instanceof InstallSnapshot) {
            receiverProvider.run(receiver -> receiver.onInstallSnapshot(this, (InstallSnapshot) message));
            return;
        }
        if (message instanceof InstallSnapshotResponse) {
            receiverProvider.run(receiver -> receiver.onInstallSnapshotResponse(this, (InstallSnapshotResponse) message));
            return;
        }

        throw new UnsupportedOperationException("Unknown message type: " + message.getClass().getName());
    }

    private void send(Object message) {
        channel.send(message);
    }

    /**
     * Closes the underlying channel side — see {@link ChannelMiddleware.ChannelSide#close()}.
     * Deliberately NOT on the four-line {@link NodeConnectionOutbound} interface: it is an
     * implementation-level teardown used by the owning node, reached via a same-package cast.
     */
    void close() {
        channel.close();
    }

    @Override
    public void sendIntroduction(Introduction introduction) {
        send(introduction);
    }

    @Override
    public void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        send(announceClusterTopology);
    }

    @Override
    public CompletableFuture<StateResponse> sendStateRequest(StateRequest stateRequest) {
        final CompletableFuture<StateResponse> response = requests.getRequestFuture(stateRequest, StateResponse.class);
        send(stateRequest);
        return response;
    }

    @Override
    public void sendStateResponse(StateResponse stateResponse) {
        send(stateResponse);
    }

    @Override
    public void sendVoteRequest(VoteRequest voteRequest) {
        send(voteRequest);
    }

    @Override
    public void sendVoteResponse(VoteResponse voteResponse) {
        send(voteResponse);
    }

    @Override
    public void sendPreVoteRequest(PreVoteRequest preVoteRequest) {
        send(preVoteRequest);
    }

    @Override
    public void sendPreVoteResponse(PreVoteResponse preVoteResponse) {
        send(preVoteResponse);
    }

    @Override
    public void sendAppendEntries(AppendEntries appendEntries) {
        send(appendEntries);
    }

    @Override
    public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        send(acknowledgeEntries);
    }

    @Override
    public void sendInstallSnapshot(InstallSnapshot installSnapshot) {
        send(installSnapshot);
    }

    @Override
    public void sendInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse) {
        send(installSnapshotResponse);
    }

    @Override
    public CompletableFuture<CacheWriteResponse> sendCachePut(CachePutRequest request) {
        final CompletableFuture<CacheWriteResponse> response = requests.getRequestFuture(request, CacheWriteResponse.class);
        send(request);
        return response;
    }

    @Override
    public CompletableFuture<CacheWriteResponse> sendCacheDelete(CacheDeleteRequest request) {
        final CompletableFuture<CacheWriteResponse> response = requests.getRequestFuture(request, CacheWriteResponse.class);
        send(request);
        return response;
    }

    @Override
    public CompletableFuture<CacheWriteResponse> sendCacheExpire(CacheExpireRequest request) {
        final CompletableFuture<CacheWriteResponse> response = requests.getRequestFuture(request, CacheWriteResponse.class);
        send(request);
        return response;
    }

    @Override
    public CompletableFuture<CacheReadResponse> sendCacheRead(CacheReadRequest request) {
        final CompletableFuture<CacheReadResponse> response = requests.getRequestFuture(request, CacheReadResponse.class);
        send(request);
        return response;
    }

    @Override
    public void sendCacheWriteResponse(CacheWriteResponse response) {
        send(response);
    }

    @Override
    public void sendCacheReadResponse(CacheReadResponse response) {
        send(response);
    }
}
