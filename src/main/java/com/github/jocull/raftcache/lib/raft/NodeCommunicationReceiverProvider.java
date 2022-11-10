package com.github.jocull.raftcache.lib.raft;

import java.util.function.Consumer;

public interface NodeCommunicationReceiverProvider {
    void run(Consumer<NodeCommunicationReceiver> fnReceiver);
}
