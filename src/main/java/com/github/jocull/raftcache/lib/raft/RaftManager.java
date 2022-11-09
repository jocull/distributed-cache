package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.TopologyDiscovery;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RaftManager implements AutoCloseable {
    private final EventBus eventBus = new EventBus();
    private final TopologyDiscovery topologyDiscovery;
    private final ChannelMiddleware channelMiddleware;
    private final List<RaftNode> managedNodes = new CopyOnWriteArrayList<>();

    public RaftManager(TopologyDiscovery topologyDiscovery,
                       ChannelMiddleware channelMiddleware) {
        this.topologyDiscovery = topologyDiscovery;
        this.channelMiddleware = channelMiddleware;
    }

    @Override
    public void close() {
        managedNodes.forEach(RaftNode::stop);
        managedNodes.clear();
    }

    public RaftNode newNode(String id, NodeAddress nodeAddress) {
        final RaftNode raftNode = new RaftNodeImpl(id, nodeAddress, topologyDiscovery, channelMiddleware, eventBus);
        managedNodes.add(raftNode);
        return raftNode;
    }

    public EventBus getEventBus() {
        return eventBus;
    }
}
