package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.*;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.NodeIdentifierState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

public class NodeCommunication {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCommunication.class);

    private final RaftNode owner;
    private final ChannelMiddleware.ChannelSide channel;

    private String remoteNodeId; // Unknown until introduction
    private volatile long currentIndex;

    public NodeCommunication(RaftNode owner,
                             ChannelMiddleware.ChannelSide channel) {
        this.owner = owner;
        this.channel = channel;

        // Register yourself into this view
        this.owner.getClusterTopology().register(new NodeIdentifierState(owner.getId(), owner.getNodeAddress()));

        this.channel.setReceiver(this::receive);
    }

    public Optional<String> getRemoteNodeId() {
        return Optional.ofNullable(remoteNodeId);
    }

    public NodeAddress getRemoteNodeAddress() {
        return channel.getAddress();
    }

    public long getCurrentIndex() {
        return currentIndex;
    }

    private void send(Object message) {
        channel.send(message);
    }

    private void receive(Object message) {
        // TODO: Initial lazy version, not very maintainable with growing number of message types
        if (message instanceof Introduction) {
            onIntroduction((Introduction) message);
        } else if (message instanceof AnnounceClusterTopology) {
            onAnnounceClusterTopology((AnnounceClusterTopology) message);
        } else if (message instanceof VoteRequest) {
            onVoteRequest((VoteRequest) message);
        } else if (message instanceof VoteResponse) {
            onVoteResponse((VoteResponse) message);
        } else if (message instanceof AppendEntries) {
            onAppendEntries((AppendEntries) message);
        } else if (message instanceof AcknowledgeEntries) {
            onAcknowledgeEntries((AcknowledgeEntries) message);
        }
    }

    public void introduce() {
        send(new Introduction(
                owner.getId(),
                owner.getNodeAddress(),
                owner.getLastReceivedIndex()
        ));
    }

    private synchronized void onIntroduction(Introduction introduction) {
        // We register data about the node that has introduced itself
        owner.getClusterTopology().register(new NodeIdentifierState(
                introduction.getId(),
                introduction.getNodeAddress()
        ));
        remoteNodeId = introduction.getId();

        // And reply with the cluster topology as we know it
        send(new AnnounceClusterTopology(
                owner.getClusterTopology().getTopology().stream()
                        .map(i -> new AnnounceClusterTopology.NodeIdentifierState(
                                i.getId(),
                                i.getNodeAddress(),
                                i.getState()
                        ))
                        .collect(Collectors.toList())
        ));
    }

    private void onAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        // When the topology has been received we can update our local view of the world
        owner.getClusterTopology().register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifierState(
                        i.getId(),
                        i.getNodeAddress(),
                        i.getState()
                ))
                .collect(Collectors.toList()));

        if (remoteNodeId == null) {
            remoteNodeId = owner.getClusterTopology().locate(channel.getAddress())
                    .map(NodeIdentifierState::getId)
                    .orElseThrow(() -> new IllegalStateException("Remote address " + channel.getAddress() + " not found in cluster toplogy: {}" + owner.getClusterTopology().getTopology()));
        }
    }

    public void requestVote(VoteRequest voteRequest) {
        send(voteRequest);
    }

    private void onVoteRequest(VoteRequest voteRequest) {
        owner.onRequestVote(remoteNodeId, voteRequest)
                .ifPresent(this::send);
    }

    private void onVoteResponse(VoteResponse voteResponse) {
        owner.onVoteResponse(remoteNodeId, voteResponse);
    }

    public void appendEntries(AppendEntries appendEntries) {
        send(appendEntries);
    }

    private void onAppendEntries(AppendEntries appendEntries) {
        final AcknowledgeEntries ack = owner.onAppendEntries(remoteNodeId, appendEntries);
        send(ack);
    }

    private void onAcknowledgeEntries(AcknowledgeEntries message) {
        if (message.getTerm() > owner.getCurrentTerm()) {
            owner.notifyTermChange(remoteNodeId, message.getTerm());
            return;
        }
        if (!message.isSuccess()) {
            LOGGER.warn("{} received AcknowledgeEntries without success from {}: {}, {}", owner.getId(), remoteNodeId, message.getTerm(), message.getCurrentIndex());
            return;
        }
        LOGGER.warn("{} received AcknowledgeEntries from {} moving index {} -> {}", owner.getId(), remoteNodeId, currentIndex, message.getCurrentIndex());
        currentIndex = message.getCurrentIndex();
        owner.updateCommittedIndex();
    }
}
