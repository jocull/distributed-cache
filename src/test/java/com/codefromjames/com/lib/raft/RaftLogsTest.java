package com.codefromjames.com.lib.raft;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RaftLogsTest {
    @Test
    void testRaftLogs() {
        final RaftLogs raftLogs = new RaftLogs();
        assertEquals(0L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final RaftLog<String> log1 = raftLogs.appendLog("hello");
        assertEquals(1L, log1.getIndex());
        assertEquals("hello", log1.getLog());
        assertEquals(String.class, log1.getLogClass());

        assertEquals(1L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final RaftLog<String> log2 = raftLogs.appendLog("world");
        assertEquals(2L, log2.getIndex());
        assertEquals("world", log2.getLog());
        assertEquals(String.class, log2.getLogClass());

        assertEquals(2L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(3L)); // Ahead of current
        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(-1L)); // Lower than commit
        assertEquals(Collections.emptyList(), raftLogs.commit(0L)); // No effect, empty result
        assertEquals(2L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final List<RaftLog<?>> commit = raftLogs.commit(log1.getIndex());
        assertEquals(2L, raftLogs.getCurrentIndex());
        assertEquals(1L, raftLogs.getCommitIndex());
        assertEquals(List.of(log1), commit);

        final List<RaftLog<?>> rollback = raftLogs.rollback();
        assertEquals(1L, raftLogs.getCurrentIndex());
        assertEquals(1L, raftLogs.getCommitIndex());
        assertEquals(List.of(log2), rollback);
    }
}