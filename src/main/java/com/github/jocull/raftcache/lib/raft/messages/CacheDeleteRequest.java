package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;

import java.io.Serializable;
import java.util.UUID;

public class CacheDeleteRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private final KeyDataDeleteInput payload;

    public CacheDeleteRequest(KeyDataDeleteInput payload) {
        this.payload = payload;
    }

    // Wire-reconstruction only — see Request(UUID).
    CacheDeleteRequest(UUID requestId, KeyDataDeleteInput payload) {
        super(requestId);
        this.payload = payload;
    }

    public KeyDataDeleteInput getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "CacheDeleteRequest{payload=" + payload + "} " + super.toString();
    }
}
