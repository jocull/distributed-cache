package com.github.jocull.raftcache.lib.raft.messages.proto;

/**
 * Proto message for adding a node to the cluster (RaftLog membership_add field 6).
 * This is used for leader-driven join with replicated membership entries in RaftLog (Stage 3, OQ-2 = a).
 * 
 * Wire format: Field number 6 inside RaftLog.oneof block.
 * Frozen rule: Node identifier string must be immutable once set.
 */
public class MemberAdd {

    private String nodeIdentifier;

    public MemberAdd() {
        this.nodeIdentifier = "";
    }

    /**
     * Constructor with node identifier
     */
    public MemberAdd(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    /**
     * Returns the Raft node identifier for this member.
     */
    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    /**
     * Sets the Raft node identifier string.
     */
    public MemberAdd setNodeIdentifier(String value) {
        this.nodeIdentifier = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberAdd that = (MemberAdd) o;
        return nodeIdentifier != null && nodeIdentifier.equals(that.nodeIdentifier);
    }

    @Override
    public int hashCode() {
        return nodeIdentifier != null ? nodeIdentifier.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MemberAdd{" +
                "nodeIdentifier='" + nodeIdentifier + '\'' +
                '}';
    }
}