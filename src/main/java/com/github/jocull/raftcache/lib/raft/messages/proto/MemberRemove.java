package com.github.jocull.raftcache.lib.raft.messages.proto;

/**
 * Proto message for removing a node from the cluster (RaftLog membership_remove field 7).
 * This is used for leader-driven join with replicated membership entries in RaftLog (Stage 3, OQ-2 = a).
 * 
 * Wire format: Field number 7 inside RaftLog.oneof block.
 * Frozen rule: Node identifier string must be immutable once set.
 */
public class MemberRemove {

    private String nodeIdentifier;

    public MemberRemove() {
        this.nodeIdentifier = "";
    }

    /**
     * Constructor with node identifier
     */
    public MemberRemove(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    /**
     * Returns the Raft node identifier for this member being removed.
     */
    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    /**
     * Sets the Raft node identifier string.
     */
    public MemberRemove setNodeIdentifier(String value) {
        this.nodeIdentifier = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberRemove that = (MemberRemove) o;
        return nodeIdentifier != null && nodeIdentifier.equals(that.nodeIdentifier);
    }

    @Override
    public int hashCode() {
        return nodeIdentifier != null ? nodeIdentifier.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MemberRemove{" +
                "nodeIdentifier='" + nodeIdentifier + '\'' +
                '}';
    }
}