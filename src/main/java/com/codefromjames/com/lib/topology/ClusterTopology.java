package com.codefromjames.com.lib.topology;

import java.util.HashMap;
import java.util.Map;

public class ClusterTopology {
    private final Map<String, NodeIdentifierState> identifiers;

    public ClusterTopology() {
        this.identifiers = new HashMap<>();
    }
}
