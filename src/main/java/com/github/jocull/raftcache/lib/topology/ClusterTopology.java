package com.github.jocull.raftcache.lib.topology;

import java.util.*;

public class ClusterTopology {
    private final Map<String, NodeIdentifier> identifiers;
    // Track pending joins: nodeId -> boolean (add = true means new member wants to join)
    private final Map<String, Boolean> pendingJoins;

    public ClusterTopology() {
        this.identifiers = new HashMap<>();
        this.pendingJoins = new HashMap<>();
    }

    /**
     * Record a pending join for nodeId. This is used when a new member calls
     * connectWithTopology() and gets topology with its own id in the view —
     * indicating it should be added to the cluster once log-derived membership
     * allows (i.e., leader appends entry and applies).
     */
    public void addPendingJoin(String nodeId) {
        pendingJoins.put(nodeId, true);
    }

    /**
     * Remove a pending join as it has been applied.
     */
    public void removePendingJoin(String nodeId) {
        pendingJoins.remove(nodeId);
    }

    /**
     * Check if there's a pending join for the given node.
     */
    public boolean hasPendingJoin(String nodeId) {
        return pendingJoins.containsKey(nodeId);
    }

    /**
     * Get the pending join status (true = add, false = remove — though removes are
     * currently handled via unregisterNode).
     */
    public Boolean getPendingJoinStatus(String nodeId) {
        return pendingJoins.get(nodeId);
    }

    public Optional<NodeIdentifier> locate(NodeAddress nodeAddress) {
        return identifiers.values().stream()
                .filter(i -> i.getNodeAddress().getAddress().equals(nodeAddress.getAddress()))
                .findFirst();
    }

    public void register(NodeIdentifier identifier) {
        identifiers.put(identifier.getId(), identifier);
    }

    public void register(Collection<NodeIdentifier> identifiers) {
        identifiers.forEach(i -> this.identifiers.put(i.getId(), i));
    }

    public NodeIdentifier unregister(String nodeId) {
        return identifiers.remove(nodeId);
    }

    public List<NodeIdentifier> getTopology() {
        return List.copyOf(identifiers.values());
    }

    public int getMajorityCount() {
        return (identifiers.size() / 2) + 1;
    }

    public int getClusterCount() {
        return identifiers.size();
    }
}
