package com.github.jocull.raftcache.lib.communication;

import com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RaftLog.MembershipEntry proto type (Stage 3, 2026-11-27).
 * Tests the generated proto class directly instead of wrapper.
 */
class MembershipEntryTest {

    @Test
    void testAddMemberConstruction() {
        RaftLog.MembershipEntry entry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("test-node")
                .setAddMember(true)
                .build();
        
        assertEquals("test-node", entry.getNodeId());
        assertTrue(entry.getAddMember());
    }

    @Test
    void testConstructorWithExplicitFlag() {
        RaftLog.MembershipEntry addEntry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("node1")
                .setAddMember(true)
                .build();
        
        RaftLog.MembershipEntry removeEntry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("node2")
                .setAddMember(false)
                .build();

        assertEquals("node1", addEntry.getNodeId());
        assertTrue(addEntry.getAddMember());
        assertEquals("node2", removeEntry.getNodeId());
        assertFalse(removeEntry.getAddMember());
    }

    @Test
    void testAsNodeIdentifier() {
        RaftLog.MembershipEntry entry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId(UUID.randomUUID().toString())
                .setAddMember(true)
                .build();
        
        NodeAddress address = new NodeAddress("test-address");
        String nodeId = entry.getNodeId();
        assertNotNull(nodeId);
    }

    @Test
    void testToString() {
        RaftLog.MembershipEntry entry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("node-123")
                .setAddMember(true)
                .build();
        
        // toString may vary in exact format but must contain key fields for proto types
        String str = entry.toString();
        assertTrue(str.contains("node-123"));
    }
}
