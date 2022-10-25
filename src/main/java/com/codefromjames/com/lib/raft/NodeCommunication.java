package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.messages.AnnounceClusterTopology;
import com.codefromjames.com.lib.raft.messages.Introduction;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.ClusterTopology;
import com.codefromjames.com.lib.topology.NodeIdentifierState;

import java.util.stream.Collectors;

public class NodeCommunication {
    private final RaftManager raftManager;
    private final NodeState self;
    private final ChannelMiddleware.ChannelSide channel;
    private final ClusterTopology clusterTopology;

    private NodeState remote = null;

    public NodeCommunication(RaftManager raftManager,
                             NodeState self,
                             ChannelMiddleware.ChannelSide channel,
                             ClusterTopology clusterTopology) {
        this.raftManager = raftManager;
        this.self = self;
        this.channel = channel;
        this.clusterTopology = clusterTopology;

        // Register yourself into this view
        this.clusterTopology.register(new NodeIdentifierState(self.getId(), self.getNodeAddress()));

        this.channel.setReceiver(this::receive);
    }

    public boolean isRemoteLeader() {
        return NodeStates.LEADER.equals(remote.getState());
    }

    public void send(Object message) {
        channel.send(message);
    }

    public void receive(Object message) {
        // TODO: Initial lazy version, not very maintainable with growing number of message types
        if (message instanceof Introduction) {
            onIntroduction((Introduction) message);
        } else if (message instanceof AnnounceClusterTopology) {
            onAnnounceClusterTopology((AnnounceClusterTopology) message);
        }
    }

    public void introduce() {
        send(new Introduction(
                self.getId(),
                self.getNodeAddress(),
                self.getLastReceivedIndex()
        ));
    }

    private void onIntroduction(Introduction introduction) {
        // We register data about the node that has introduced itself
        clusterTopology.register(new NodeIdentifierState(
                introduction.getId(),
                introduction.getNodeAddress()
        ));
        remote = new NodeState(introduction.getId(), introduction.getNodeAddress())
                .setLastReceivedIndex(introduction.getLastReceivedIndex());

        // And reply with the cluster topology as we know it
        send(new AnnounceClusterTopology(
                clusterTopology.getTopology().stream()
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
        clusterTopology.register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifierState(
                        i.getId(),
                        i.getNodeAddress(),
                        i.getState()
                ))
                .collect(Collectors.toList()));

        if (remote == null) {
            remote = clusterTopology.locate(channel.getAddress())
                    .map(i -> new NodeState(i.getId(), i.getNodeAddress()))
                    .orElseThrow(() -> new IllegalStateException("Remote address " + channel.getAddress() + " not found in cluster toplogy: {}" + clusterTopology.getTopology()));
        }
    }
}
