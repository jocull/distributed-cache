package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

import java.io.Serializable;

public class CacheWriteResponse extends Response implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CacheStatus status;
    private final NodeAddress leaderHint;

    // `leaderHint` is nullable: under the B first cut it is always absent; task C's
    // chosen variant (forward vs redirect) may name the node the client should re-target.
    public CacheWriteResponse(Request request, CacheStatus status, NodeAddress leaderHint) {
        super(request);
        this.status = status;
        this.leaderHint = leaderHint;
    }

    public CacheStatus getStatus() {
        return status;
    }

    public NodeAddress getLeaderHint() {
        return leaderHint;
    }

    @Override
    public String toString() {
        return "CacheWriteResponse{" +
                "status=" + status +
                ", leaderHint=" + leaderHint +
                "} " + super.toString();
    }
}
