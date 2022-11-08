package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeConnectionOutboundImpl implements NodeConnectionOutbound {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeConnectionOutboundImpl.class);

    private String remoteNodeId;
    private TermIndex currentTermIndex = new TermIndex(0, 0L);

    private final NodeCommunicationReceiver receiver;
    private final ChannelMiddleware.ChannelSide channel;

    public NodeConnectionOutboundImpl(NodeCommunicationReceiver receiver,
                                      ChannelMiddleware.ChannelSide channel) {
        this.receiver = receiver;
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
            receiver.onIntroduction(this, (Introduction) message);
            return;
        }
        if (message instanceof AnnounceClusterTopology) {
            receiver.onAnnounceClusterTopology(this, (AnnounceClusterTopology) message);
            return;
        }
        if (message instanceof StateRequest) {
            receiver.onStateRequest(this, (StateRequest) message);
            return;
        }
        if (message instanceof StateResponse) {
            receiver.
                    onStateResponse(this, (StateResponse) message);
            return;
        }
        if (remoteNodeId == null) {
            throw new IllegalStateException("Cannot process " + message.getClass().getSimpleName()
                    + " before introduction! remoteNodeId is null");
        }

        // After introductions, any message can process
        if (message instanceof VoteRequest) {
            receiver.onVoteRequest(this, (VoteRequest) message);
            return;
        }
        if (message instanceof VoteResponse) {
            receiver.onVoteResponse(this, (VoteResponse) message);
            return;
        }
        if (message instanceof AppendEntries) {
            receiver.onAppendEntries(this, (AppendEntries) message);
            return;
        }
        if (message instanceof AcknowledgeEntries) {
            receiver.onAcknowledgeEntries(this, (AcknowledgeEntries) message);
            return;
        }

        throw new UnsupportedOperationException("Unknown message type: " + message.getClass().getName());
    }

    private void send(Object message) {
        channel.send(message);
    }

    @Override
    public void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        send(announceClusterTopology);
    }

    @Override
    public void sendStateResponse(StateResponse stateResponse) {
        send(stateResponse);
    }

    @Override
    public void sendVoteResponse(VoteResponse voteResponse) {
        send(voteResponse);
    }

    @Override
    public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        send(acknowledgeEntries);
    }
}
