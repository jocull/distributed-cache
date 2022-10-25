package com.codefromjames.com.lib.topology;

import java.util.*;

public class ClusterTopology {
    private final Map<String, NodeIdentifierState> identifiers;

    public ClusterTopology() {
        this.identifiers = new HashMap<>();
    }

    public synchronized Optional<NodeIdentifierState> locate(NodeAddress nodeAddress) {
        return identifiers.values().stream()
                .filter(i -> i.getNodeAddress().getAddress().equals(nodeAddress.getAddress()))
                .findFirst();
    }

    public synchronized void register(NodeIdentifierState identifier) {
        identifiers.put(identifier.getId(), identifier);
    }

    public synchronized void register(Collection<NodeIdentifierState> identifiers) {
        identifiers.forEach(i -> this.identifiers.put(i.getId(), i));
    }

    public synchronized NodeIdentifierState unregister(String nodeId) {
        return identifiers.remove(nodeId);
    }

    public synchronized List<NodeIdentifierState> getTopology() {
        return List.copyOf(identifiers.values());
    }

    public synchronized int getMajorityCount() {
        return (identifiers.size() / 2) + 1;
    }
}
