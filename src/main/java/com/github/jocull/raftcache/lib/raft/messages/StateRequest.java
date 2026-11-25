package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

public class StateRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;

    public StateRequest() {
    }

    // Wire-reconstruction only — see Request(UUID).
    StateRequest(UUID requestId) {
        super(requestId);
    }

    @Override
    public String toString() {
        return "StateRequest{} " + super.toString();
    }
}
