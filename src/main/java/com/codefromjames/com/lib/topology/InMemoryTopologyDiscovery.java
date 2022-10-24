package com.codefromjames.com.lib.topology;

import java.util.ArrayList;
import java.util.List;

public class InMemoryTopologyDiscovery implements TopologyDiscovery {
    private final List<NodeAddress> knownNodes = new ArrayList<>();

    public synchronized void addKnownNode(NodeAddress nodeAddress) {
        knownNodes.add(nodeAddress);
    }

    public synchronized boolean removeKnownNode(NodeAddress nodeAddress) {
        return knownNodes.remove(nodeAddress);
    }

    @Override
    public List<NodeAddress> getNodes() {
        return List.copyOf(knownNodes);
    }
}
