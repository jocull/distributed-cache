package com.github.jocull.raftcache.lib.raft.messages;

import java.util.UUID;

public class Response {
    private final UUID requestId;

    public Response(Request request) {
        this.requestId = request.getRequestId();
    }

    public UUID getRequestId() {
        return requestId;
    }

    @Override
    public String toString() {
        return "Response{" +
                "requestId=" + requestId +
                '}';
    }
}
