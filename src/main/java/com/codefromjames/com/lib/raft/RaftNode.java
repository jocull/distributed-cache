package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.ClusterTopology;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.NodeIdentifierState;

import java.util.List;

public class RaftNode {
    private final NodeState self;
    private final RaftManager manager;

    private final ClusterTopology clusterTopology;

    public RaftNode(NodeState self,
                    RaftManager manager) {
        this.self = self;
        this.manager = manager;

        // Each node has its own view of cluster topology
        clusterTopology = new ClusterTopology();
    }

    public NodeState getSelf() {
        return self;
    }

    public NodeCommunication connectTo(NodeAddress remoteAddress) {
        return new NodeCommunication(manager, self, manager.openChannel(this, remoteAddress), clusterTopology);
    }

    public NodeCommunication onConnection(ChannelMiddleware.ChannelSide connection) {
        return new NodeCommunication(manager, self, connection, clusterTopology);
    }

    public NodeAddress getNodeAddress() {
        return self.getNodeAddress();
    }

    public List<NodeIdentifierState> getTopology() {
        return clusterTopology.getTopology();
    }
}
