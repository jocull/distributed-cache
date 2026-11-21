package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;

public class StateRequest extends Request implements Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public String toString() {
        return "StateRequest{} " + super.toString();
    }
}
