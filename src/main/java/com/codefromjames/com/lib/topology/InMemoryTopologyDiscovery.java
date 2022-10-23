package com.codefromjames.com.lib.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryTopologyDiscovery implements TopologyDiscovery {
    private final List<NodeAddress> knownNodes = new ArrayList<>();

    public void addKnownNode(NodeAddress nodeAddress) {
        knownNodes.add(nodeAddress);
    }

    public boolean removeKnownNode(NodeAddress nodeAddress) {
        return knownNodes.remove(nodeAddress);
    }

    @Override
    public List<NodeAddress> getNodes() {
        return Collections.unmodifiableList(knownNodes);
    }
}
