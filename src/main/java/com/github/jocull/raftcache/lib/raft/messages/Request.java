package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

public class Request implements Serializable {
    private static final long serialVersionUID = 1L;
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
