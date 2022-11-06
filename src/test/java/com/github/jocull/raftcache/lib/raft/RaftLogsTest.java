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
        assertEquals(0, raftLogs.getCurrentTermIndex().getTerm());
        assertEquals(0L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0, raftLogs.getCommittedTermIndex().getTerm());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        final RaftLog<String> log1 = raftLogs.appendLog(1, "hello");
        assertEquals(1, log1.getTermIndex().getTerm());
        assertEquals(1L, log1.getTermIndex().getIndex());
        assertEquals("hello", log1.getEntry());
        assertEquals(String.class, log1.getLogClass());

        assertEquals(1L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        final RaftLog<String> log2 = raftLogs.appendLog(1, "world");
        assertEquals(2L, log2.getTermIndex().getIndex());
        assertEquals("world", log2.getEntry());
        assertEquals(String.class, log2.getLogClass());

        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(new TermIndex(1, 3L))); // Ahead of current
        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(new TermIndex(1, -1L))); // Lower than commit
        assertEquals(Collections.emptyList(), raftLogs.commit(new TermIndex(1, 0L))); // No effect, empty result
        assertEquals(1, raftLogs.getCurrentTermIndex().getTerm());
        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(1, raftLogs.getCommittedTermIndex().getTerm());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        final List<RaftLog<?>> commit = raftLogs.commit(log1.getTermIndex());
        assertEquals(1, raftLogs.getCurrentTermIndex().getTerm());
        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(1, raftLogs.getCommittedTermIndex().getTerm());
        assertEquals(1L, raftLogs.getCommittedTermIndex().getIndex());
        assertEquals(List.of(log1), commit);

        final List<RaftLog<?>> rollback = raftLogs.rollback();
        assertEquals(1, raftLogs.getCurrentTermIndex().getTerm());
        assertEquals(1L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(1, raftLogs.getCommittedTermIndex().getTerm());
        assertEquals(1L, raftLogs.getCommittedTermIndex().getIndex());
        assertEquals(List.of(log2), rollback);
    }

    @Test
    void testAppendTermChange() {
        final RaftLogs raftLogs = new RaftLogs();

        final RaftLog<String> log1 = raftLogs.appendLog(new TermIndex(1, 1), "hello");
        final RaftLog<String> log2 = raftLogs.appendLog(new TermIndex(1, 1), "hello");
        assertSame(log1, log2);

        final RaftLog<String> log3 = raftLogs.appendLog(new TermIndex(1, 2), "world");
        final RaftLog<String> log4 = raftLogs.appendLog(new TermIndex(1, 3), "universe");
        final List<RaftLog<?>> range1 = raftLogs.getLogRange(new TermIndex(0, 0), 25);
        assertEquals(List.of(log1, log3, log4), range1);

        final RaftLog<String> log5 = raftLogs.appendLog(new TermIndex(2, 2), "world");
        final RaftLog<String> log6 = raftLogs.appendLog(new TermIndex(2, 3), "universe");
        final List<RaftLog<?>> range2 = raftLogs.getLogRange(new TermIndex(0, 0), 25);
        assertEquals(List.of(log1, log5, log6), range2);
    }

    @Test
    void testAppendLogGap() {
        final RaftLogs raftLogs = new RaftLogs();

        raftLogs.appendLog(new TermIndex(1, 1), "hello");
        raftLogs.appendLog(new TermIndex(1, 3), "universe");
        assertThrows(IllegalStateException.class, () -> raftLogs.appendLog(new TermIndex(1, 2), "world"));
    }

    @Test
    void testFutureRaftLogsCommit() throws ExecutionException, InterruptedException, TimeoutException {
        final RaftLogs raftLogs = new RaftLogs();
        assertEquals(0, raftLogs.getCurrentTermIndex().getTerm());
        assertEquals(0L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0, raftLogs.getCommittedTermIndex().getTerm());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        final AtomicReference<Thread> futureThread = new AtomicReference<>();
        final AtomicBoolean futureRan = new AtomicBoolean(false);
        final CompletableFuture<Void> log1 = raftLogs.appendFutureLog(new TermIndex(1, 1), "hello")
                .thenAccept(log -> {
                    assertEquals(new TermIndex(1, 1), log.getTermIndex());
                    assertEquals("hello", log.getEntry());
                    assertEquals(String.class, log.getLogClass());
                    futureThread.set(Thread.currentThread());
                    futureRan.set(true);
                });

        assertEquals(1L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());
        assertFalse(log1.isCancelled());
        assertFalse(log1.isDone());
        assertFalse(log1.isCompletedExceptionally());
        assertNull(futureThread.get());
        assertFalse(futureRan.get());

        final List<RaftLog<?>> commit = raftLogs.commit(new TermIndex(1, 1));
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
        assertEquals(0L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());

        final AtomicReference<Thread> futureThread = new AtomicReference<>();
        final AtomicBoolean futureAcceptRan = new AtomicBoolean(false);
        final AtomicBoolean futureExceptionallyRan = new AtomicBoolean(false);
        final CompletableFuture<Void> log1 = raftLogs.appendFutureLog(new TermIndex(1, 1), "hello")
                .thenAccept(log -> {
                    futureAcceptRan.set(true);
                });

        log1.exceptionally(throwable -> {
            futureThread.set(Thread.currentThread());
            futureExceptionallyRan.set(true);
            return null;
        });

        assertEquals(1L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(0L, raftLogs.getCommittedTermIndex().getIndex());
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