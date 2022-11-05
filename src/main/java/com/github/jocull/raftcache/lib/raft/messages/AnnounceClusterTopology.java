package com.github.jocull.raftcache.lib.raft.messages;

import java.util.List;

public class AnnounceClusterTopology {
    public final List<NodeIdentifier> nodeIdentifiers;

    public AnnounceClusterTopology(List<NodeIdentifier> nodeIdentifiers) {
        this.nodeIdentifiers = List.copyOf(nodeIdentifiers);
    }

    public List<NodeIdentifier> getNodeIdentifierStates() {
        return nodeIdentifiers;
    }

}
