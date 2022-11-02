package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.topology.NodeAddress;

import java.util.function.Consumer;

public interface ChannelMiddleware {
    ChannelSide openChannel(RaftNode source, NodeAddress targetAddress);

    // TODO: Do we really need this interface? It doesn't appear useful yet.
    interface ChannelPair {
        ChannelSide getLeft();
        ChannelSide getRight();
    }

    interface ChannelSide {
        NodeAddress getAddress();
        void send(Object message);
        void setReceiver(Consumer<Object> receiver);
    }
}
