package com.github.jocull.raftcache.lib.raft.messages;

import java.util.UUID;

public class Request {
    private final UUID requestId = UUID.randomUUID();

    public UUID getRequestId() {
        return requestId;
    }

    @Override
    public String toString() {
        return "Request{" +
                "requestId=" + requestId +
                '}';
    }
}
