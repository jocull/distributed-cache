package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * A request to join or leave a cluster member (membership change operation).
 * S1: carries requestId, nodeAddress (the member address).
 */
public class ClusterJoinRequest extends Request implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The node identifier being added or removed (member address) */
    private String nodeAddress;

    public ClusterJoinRequest() {
        // Default constructor: generates UUID for wire reconstruction
        this.nodeAddress = "";
    }

    /**
     * Constructor for join/leave operation.
     * @param requestId UUID string passed from client (for request tracking)
     * @param nodeAddress host:port of the member to add/remove
     */
    public ClusterJoinRequest(String requestId, String nodeAddress) {
        // Pass UUID to parent; then initialize our field
        super(requestId != null ? UUID.fromString(requestId) : UUID.randomUUID());
        this.nodeAddress = nodeAddress;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

}