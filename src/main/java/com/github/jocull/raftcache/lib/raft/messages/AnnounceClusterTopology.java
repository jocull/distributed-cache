package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.List;

public class AnnounceClusterTopology implements Serializable {
    private static final long serialVersionUID = 1L;

    public final List<NodeIdentifier> nodeIdentifiers;

    public AnnounceClusterTopology(List<NodeIdentifier> nodeIdentifiers) {
        this.nodeIdentifiers = List.copyOf(nodeIdentifiers);
    }

    public List<NodeIdentifier> getNodeIdentifierStates() {
        return nodeIdentifiers;
    }

}
