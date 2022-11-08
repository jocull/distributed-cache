package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

public interface NodeCommunicationState {
    String getRemoteNodeId();

    void setRemoteNodeId(String remoteNodeId);

    NodeAddress getRemoteNodeAddress();

    TermIndex getTermIndex();

    void setTermIndex(TermIndex termIndex);
}
