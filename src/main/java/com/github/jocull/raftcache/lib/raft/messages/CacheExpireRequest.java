package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;

import java.io.Serializable;
import java.util.UUID;

public class CacheExpireRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private final KeyDataExpireInput payload;

    public CacheExpireRequest(KeyDataExpireInput payload) {
        this.payload = payload;
    }

    // Wire-reconstruction only — see Request(UUID).
    CacheExpireRequest(UUID requestId, KeyDataExpireInput payload) {
        super(requestId);
        this.payload = payload;
    }

    public KeyDataExpireInput getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "CacheExpireRequest{payload=" + payload + "} " + super.toString();
    }
}
