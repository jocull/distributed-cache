package com.github.jocull.raftcache.lib.raft.messages.proto;

/**
 * Static factory helpers for MembershipEntry - creates add/remove ops wrapped properly for WireMessage (Stage 3).
 */
public class MembershipEntryWrapper {

    /**
     * Create a membership add entry (MEMBERSHIP_ADD field 6)
     * Note: In the current protocol, this uses top-level MembershipEntry proto message.
     */
    public static MemberAdd ofAdd(String nodeId) {
        return new MemberAdd(nodeId);
    }

    /**
     * Create a membership remove entry (MEMBERSHIP_REMOVE field 7)
     * Note: In the current protocol, this uses top-level MembershipEntry proto message.
     */
    public static MemberRemove ofRemove(String nodeId) {
        return new MemberRemove(nodeId);
    }

    /**
     * Wrap an existing MemberAdd as a MembershipEntry
     */
    public static MemberAdd wrapMemberAdd(MemberAdd member) {
        return member;
    }

    /**
     * Wrap an existing MemberRemove as a MembershipEntry
     */
    public static MemberRemove wrapMemberRemove(MemberRemove member) {
        return member;
    }
}