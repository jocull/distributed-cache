package com.github.jocull.raftcache.lib.topology;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InMemoryTopologyDiscovery implements TopologyDiscovery {
    private final Set<NodeAddress> knownNodes = new HashSet<>();

    public synchronized InMemoryTopologyDiscovery addKnownNode(NodeAddress nodeAddress) {
        knownNodes.add(nodeAddress);
        return this;
    }

    public synchronized boolean removeKnownNode(NodeAddress nodeAddress) {
        return knownNodes.remove(nodeAddress);
    }

    @Override
    public List<NodeAddress> getNodes() {
        return List.copyOf(knownNodes);
    }
}
