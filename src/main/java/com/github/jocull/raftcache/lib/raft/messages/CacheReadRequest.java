package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

public class CacheReadRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;

    public CacheReadRequest(String key) {
        this.key = key;
    }

    // Wire-reconstruction only — see Request(UUID).
    CacheReadRequest(UUID requestId, String key) {
        super(requestId);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "CacheReadRequest{key=" + key + "} " + super.toString();
    }
}
