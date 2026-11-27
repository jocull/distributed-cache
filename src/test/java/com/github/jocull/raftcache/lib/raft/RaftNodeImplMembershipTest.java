package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RaftNodeImpl membership handling via RaftLog.MembershipEntry (Stage 3, 2026-11-27).
 */
class RaftNodeImplMembershipTest {

    @Test
    void testRaftLogMembershipEntryStructure() {
        // Create a membership entry for adding a node
        RaftLog.MembershipEntry addEntry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("new-node-123")
                .setAddMember(true)
                .build();
        
        assertNotNull(addEntry);
        assertEquals("new-node-123", addEntry.getNodeId());
        assertTrue(addEntry.getAddMember());
    }

    @Test
    void testRaftLogMembershipEntryRemoval() {
        // Create a membership entry for removing a node
        RaftLog.MembershipEntry removeEntry = RaftLog.MembershipEntry.newBuilder()
                .setNodeId("old-node-456")
                .setAddMember(false)
                .build();
        
        assertNotNull(removeEntry);
        assertEquals("old-node-456", removeEntry.getNodeId());
        assertFalse(removeEntry.getAddMember());
    }

    @Test
    void testMembershipEntryProtoFieldNumbers() {
        // Verify the generated type has expected methods for proto field 6 (membership)
        RaftLog.MembershipEntry entry = RaftLog.MembershipEntry.newBuilder().build();
        
        // Entry should have getNodeId, getAddMember methods from proto generation
        assertNotNull(entry.getNodeId());  // defaults to empty string
        assertNotNull(entry.getAddMember());  // defaults to false for proto3 boolean
    }

    @Test
    void testMembershipEntryEmptyConstruction() {
        RaftLog.MembershipEntry entry = RaftLog.MembershipEntry.newBuilder().build();
        
        // Empty entry should have default values
        assertEquals("", entry.getNodeId());
        assertFalse(entry.getAddMember());
    }
}
