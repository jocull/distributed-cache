package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.communication.KeyDataOutput;

import java.io.Serializable;

public class CacheReadResponse extends Response implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean found;
    private final KeyDataOutput value;

    // `value` is null when {@code found} is false (the key is not in this node's local
    // view — B §4.3 local-serving reads); when found it carries key + data + version.
    public CacheReadResponse(Request request, boolean found, KeyDataOutput value) {
        super(request);
        this.found = found;
        this.value = value;
    }

    public boolean isFound() {
        return found;
    }

    public KeyDataOutput getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CacheReadResponse{" +
                "found=" + found +
                ", value=" + value +
                "} " + super.toString();
    }
}
