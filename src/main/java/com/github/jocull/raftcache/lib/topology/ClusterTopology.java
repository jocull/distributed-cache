package com.github.jocull.raftcache.lib.topology;

import java.util.*;

public class ClusterTopology {
    private final Map<String, NodeIdentifier> identifiers;

    public ClusterTopology() {
        this.identifiers = new HashMap<>();
    }

    public synchronized Optional<NodeIdentifier> locate(NodeAddress nodeAddress) {
        return identifiers.values().stream()
                .filter(i -> i.getNodeAddress().getAddress().equals(nodeAddress.getAddress()))
                .findFirst();
    }

    public synchronized void register(NodeIdentifier identifier) {
        identifiers.put(identifier.getId(), identifier);
    }

    public synchronized void register(Collection<NodeIdentifier> identifiers) {
        identifiers.forEach(i -> this.identifiers.put(i.getId(), i));
    }

    public synchronized NodeIdentifier unregister(String nodeId) {
        return identifiers.remove(nodeId);
    }

    public synchronized List<NodeIdentifier> getTopology() {
        return List.copyOf(identifiers.values());
    }

    public synchronized int getMajorityCount() {
        return (identifiers.size() / 2) + 1;
    }

    public synchronized int getClusterCount() {
        return identifiers.size();
    }
}
