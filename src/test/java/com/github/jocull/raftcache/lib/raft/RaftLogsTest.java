package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class RaftLogsTest {
    @Test
    void testRaftLogs() {
        final RaftLogs raftLogs = new RaftLogs();
        assertEquals(0L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final RaftLog<String> log1 = raftLogs.appendLog(1, "hello");
        assertEquals(1L, log1.getIndex());
        assertEquals("hello", log1.getEntry());
        assertEquals(String.class, log1.getLogClass());

        assertEquals(1L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final RaftLog<String> log2 = raftLogs.appendLog(1, "world");
        assertEquals(2L, log2.getIndex());
        assertEquals("world", log2.getEntry());
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

    @Test
    void testAppendTermChange() {
        final RaftLogs raftLogs = new RaftLogs();

        final RaftLog<String> log1 = raftLogs.appendLog(1, 1, "hello");
        final RaftLog<String> log2 = raftLogs.appendLog(1, 1, "hello");
        assertSame(log1, log2);

        final RaftLog<String> log3 = raftLogs.appendLog(1, 2, "world");
        final RaftLog<String> log4 = raftLogs.appendLog(1, 3, "universe");
        final List<RaftLog<?>> range1 = raftLogs.getLogRange(0, 25);
        assertEquals(List.of(log1, log3, log4), range1);

        final RaftLog<String> log5 = raftLogs.appendLog(2, 2, "world");
        final RaftLog<String> log6 = raftLogs.appendLog(2, 3, "universe");
        final List<RaftLog<?>> range2 = raftLogs.getLogRange(0, 25);
        assertEquals(List.of(log1, log5, log6), range2);
    }

    @Test
    void testAppendLogGap() {
        final RaftLogs raftLogs = new RaftLogs();

        raftLogs.appendLog(1, 1, "hello");
        raftLogs.appendLog(1, 3, "universe");
        assertThrows(IllegalStateException.class, () -> raftLogs.appendLog(1, 2, "world"));
    }

    @Test
    void testFutureRaftLogsCommit() throws ExecutionException, InterruptedException, TimeoutException {
        final RaftLogs raftLogs = new RaftLogs();
        assertEquals(0L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final AtomicReference<Thread> futureThread = new AtomicReference<>();
        final AtomicBoolean futureRan = new AtomicBoolean(false);
        final CompletableFuture<Void> log1 = raftLogs.appendFutureLog(1, 1, "hello")
                .thenAccept(log -> {
                    assertEquals(1L, log.getIndex());
                    assertEquals("hello", log.getEntry());
                    assertEquals(String.class, log.getLogClass());
                    futureThread.set(Thread.currentThread());
                    futureRan.set(true);
                });

        assertEquals(1L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());
        assertFalse(log1.isCancelled());
        assertFalse(log1.isDone());
        assertFalse(log1.isCompletedExceptionally());
        assertNull(futureThread.get());
        assertFalse(futureRan.get());

        final List<RaftLog<?>> commit = raftLogs.commit(1);
        assertEquals(1, commit.size());
        log1.get(100, TimeUnit.MILLISECONDS);
        assertTrue(futureRan.get());
        assertNotNull(futureThread.get());
        assertNotEquals(Thread.currentThread(), futureThread.get());
        assertFalse(log1.isCancelled());
        assertTrue(log1.isDone());
        assertFalse(log1.isCompletedExceptionally());
    }

    @Test
    void testFutureRaftLogsRollback() {
        final RaftLogs raftLogs = new RaftLogs();
        assertEquals(0L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());

        final AtomicReference<Thread> futureThread = new AtomicReference<>();
        final AtomicBoolean futureAcceptRan = new AtomicBoolean(false);
        final AtomicBoolean futureExceptionallyRan = new AtomicBoolean(false);
        final CompletableFuture<Void> log1 = raftLogs.appendFutureLog(1, 1, "hello")
                .thenAccept(log -> {
                    futureAcceptRan.set(true);
                });

        log1.exceptionally(throwable -> {
            futureThread.set(Thread.currentThread());
            futureExceptionallyRan.set(true);
            return null;
        });

        assertEquals(1L, raftLogs.getCurrentIndex());
        assertEquals(0L, raftLogs.getCommitIndex());
        assertFalse(log1.isCancelled());
        assertFalse(log1.isDone());
        assertFalse(log1.isCompletedExceptionally());

        final List<RaftLog<?>> rollback = raftLogs.rollback();
        assertEquals(1, rollback.size());

        final ExecutionException ex = assertThrows(ExecutionException.class, () -> log1.get(100, TimeUnit.MILLISECONDS));
        assertNotNull(ex.getCause());
        assertEquals(CancellationException.class, ex.getCause().getClass());

        assertFalse(log1.isCancelled()); // confusingly, this future is not cancelled - the parent of it was!
        assertTrue(log1.isDone());
        assertTrue(log1.isCompletedExceptionally()); // Because CancellationException
        assertFalse(futureAcceptRan.get());
        assertNotEquals(Thread.currentThread(), futureThread.get());
        assertTrue(futureExceptionallyRan.get());
    }
}