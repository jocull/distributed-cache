package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.communication.KeyDataInput;

import java.io.Serializable;
import java.util.UUID;

public class CachePutRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private final KeyDataInput payload;

    public CachePutRequest(KeyDataInput payload) {
        this.payload = payload;
    }

    // Wire-reconstruction only — see Request(UUID).
    CachePutRequest(UUID requestId, KeyDataInput payload) {
        super(requestId);
        this.payload = payload;
    }

    public KeyDataInput getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "CachePutRequest{payload=" + payload + "} " + super.toString();
    }
}
