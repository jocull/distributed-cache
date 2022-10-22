package com.codefromjames.com.lib.communication.raft;

import java.util.HashMap;
import java.util.Map;

public class ClusterTopology {
    private final Map<String, NodeIdentifier> identifiers;

    public ClusterTopology() {
        this.identifiers = new HashMap<>();
    }
}
