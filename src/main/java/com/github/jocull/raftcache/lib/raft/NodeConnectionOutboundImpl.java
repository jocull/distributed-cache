package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class NodeConnectionOutboundImpl implements NodeConnectionOutbound {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeConnectionOutboundImpl.class);

    private String remoteNodeId;
    private TermIndex currentTermIndex = new TermIndex(0, 0L);

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
        if (message instanceof AppendEntries) {
            receiverProvider.run(receiver -> receiver.onAppendEntries(this, (AppendEntries) message));
            return;
        }
        if (message instanceof AcknowledgeEntries) {
            receiverProvider.run(receiver -> {
                requests.completeRequest((AcknowledgeEntries) message);
                receiver.onAcknowledgeEntries(this, (AcknowledgeEntries) message);
            });
            return;
        }

        throw new UnsupportedOperationException("Unknown message type: " + message.getClass().getName());
    }

    private void send(Object message) {
        channel.send(message);
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
    public CompletableFuture<AcknowledgeEntries> sendAppendEntries(AppendEntries appendEntries) {
        final CompletableFuture<AcknowledgeEntries> response = requests.getRequestFuture(appendEntries, AcknowledgeEntries.class);
        send(appendEntries);
        return response;
    }

    @Override
    public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        send(acknowledgeEntries);
    }
}
