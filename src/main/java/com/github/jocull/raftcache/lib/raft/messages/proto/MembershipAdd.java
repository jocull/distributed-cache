package com.github.jocull.raftcache.lib.raft.messages.proto;

import java.io.Serializable;

/**
 * Wrapper for membership add operation - provides MembershipEntry-compatible type (Stage 3).
 */
public class MembershipAdd implements Serializable {
    private static final long serialVersionUID = 1L;
    private final com.github.jocull.raftcache.lib.raft.messages.proto.MemberAdd member;

    public MembershipAdd(com.github.jocull.raftcache.lib.raft.messages.proto.MemberAdd member) {
        this.member = member;
    }

    public com.github.jocull.raftcache.lib.raft.messages.proto.MemberAdd getMember() {
        return member;
    }

    @Override
    public String toString() {
        return "MembershipAdd{" + member.getNodeIdentifier() + "}";
    }
}
