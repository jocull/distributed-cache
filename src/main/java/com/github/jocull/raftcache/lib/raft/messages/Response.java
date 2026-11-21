package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

public class Response implements Serializable {
    private static final long serialVersionUID = 1L;
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
