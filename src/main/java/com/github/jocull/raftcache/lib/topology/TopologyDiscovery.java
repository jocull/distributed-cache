package com.github.jocull.raftcache.lib.topology;

import java.util.List;

public interface TopologyDiscovery {
    List<NodeAddress> getNodes();
}
