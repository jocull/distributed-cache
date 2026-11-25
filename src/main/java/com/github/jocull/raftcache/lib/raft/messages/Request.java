package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

public class Request implements Serializable {
    private static final long serialVersionUID = 1L;
    private final UUID requestId;

    public Request() {
        this.requestId = UUID.randomUUID();
    }

    // Wire-reconstruction only (WireCodec lives in this package and must restore the
    // exact UUID: NodeRequests correlates StateResponses by request ID).
    Request(UUID requestId) {
        this.requestId = requestId;
    }

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
