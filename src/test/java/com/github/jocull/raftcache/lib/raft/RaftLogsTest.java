package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.event.EventBus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class RaftLogsTest {
    @Test
    void testRaftLogs() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
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
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));

        final RaftLog<String> log1 = raftLogs.appendLog(new TermIndex(1, 1), "hello");
        final RaftLog<String> log2 = raftLogs.appendLog(new TermIndex(1, 1), "hello");
        assertSame(log1, log2);

        final RaftLog<String> log3 = raftLogs.appendLog(new TermIndex(1, 2), "world");
        final RaftLog<String> log4 = raftLogs.appendLog(new TermIndex(1, 3), "universe");
        final List<RaftLog<?>> range1 = raftLogs.getLogRange(TermIndex.EPOCH, 25);
        assertEquals(List.of(log1, log3, log4), range1);

        final RaftLog<String> log5 = raftLogs.appendLog(new TermIndex(2, 2), "world");
        final RaftLog<String> log6 = raftLogs.appendLog(new TermIndex(2, 3), "universe");
        final List<RaftLog<?>> range2 = raftLogs.getLogRange(TermIndex.EPOCH, 25);
        assertEquals(List.of(log1, log5, log6), range2);
    }

    @Test
    void testAppendLogGap() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));

        raftLogs.appendLog(new TermIndex(1, 1), "hello");
        raftLogs.appendLog(new TermIndex(1, 3), "universe");
        assertThrows(IllegalStateException.class, () -> raftLogs.appendLog(new TermIndex(1, 2), "world"));
    }

    @Test
    void testFutureRaftLogsCommit() throws ExecutionException, InterruptedException, TimeoutException {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
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

        // commit() alone must NOT acknowledge the client: the caller is expected to apply the
        // entry to the state machine (publish LogsCommitted) before calling notifyCommitted,
        // which is what keeps a client from ever observing a read older than its own write
        assertFalse(log1.isDone(), "commit() moves the commit index but must not complete client futures");
        assertFalse(futureRan.get(), "no client handler may run before notifyCommitted");

        raftLogs.notifyCommitted(commit);
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
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
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

    @Test
    void testCompactKeepsTailAndBoundaryIsStartPoint() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a"); // (1,1)
        raftLogs.appendLog(1, "b"); // (1,2)
        raftLogs.appendLog(1, "c"); // (1,3)
        raftLogs.appendLog(1, "d"); // (1,4)
        raftLogs.commit(new TermIndex(1, 2L));

        // Fresh log: no snapshot at the start point
        assertFalse(raftLogs.hasStartPointSnapshot());
        assertEquals(TermIndex.EPOCH, raftLogs.getStartTermIndex());
        assertNull(raftLogs.getSnapshotPayload());

        raftLogs.compact(new TermIndex(1, 2L), "payload");

        assertTrue(raftLogs.hasStartPointSnapshot());
        assertEquals(new TermIndex(1, 2L), raftLogs.getStartTermIndex());
        assertEquals("payload", raftLogs.getSnapshotPayload());

        // Tail and commit pointers are untouched by compaction
        assertEquals(4L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(2L, raftLogs.getCommittedTermIndex().getIndex());

        // The boundary is now the start point; the compacted region is gone
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        assertFalse(raftLogs.containsStartPoint(new TermIndex(1, 1L)));

        // Ranges never serve the marker as content
        final List<RaftLog<?>> range = raftLogs.getLogRange(TermIndex.EPOCH, 25);
        assertEquals(2, range.size());
        assertEquals("c", range.get(0).getEntry());
        assertEquals("d", range.get(1).getEntry());
    }

    @Test
    void testRollbackStopsAtBoundary() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a");
        raftLogs.appendLog(1, "b");
        raftLogs.appendLog(1, "c");
        raftLogs.commit(new TermIndex(1, 2L));
        raftLogs.compact(new TermIndex(1, 2L), "payload");

        // Demotion rolls back only uncommitted entries; the boundary marker survives (S5)
        final List<RaftLog<?>> removed = raftLogs.rollback();
        assertEquals(1, removed.size());
        assertEquals("c", removed.get(0).getEntry());

        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        // With the recorded tail pulled down to committed, the reported tail stays at the
        // start point instead of dropping past it (S3)
        assertEquals(new TermIndex(1, 2L), raftLogs.getCurrentTermIndex());

        // The log keeps working past the boundary
        final RaftLog<String> c2 = raftLogs.appendLog(1, "c2");
        assertEquals(3L, c2.getTermIndex().getIndex());
        assertEquals(3L, raftLogs.getCurrentTermIndex().getIndex());
    }

    @Test
    void testCompactInstallBeyondTail() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a");
        raftLogs.appendLog(1, "b");
        raftLogs.rollback(); // discard the uncommitted tail -> recorded tail back at the epoch

        // The snapshot at (1,2) sits beyond the local tail: a full install (S2)
        raftLogs.compact(new TermIndex(1, 2L), "payload");

        assertEquals(new TermIndex(1, 2L), raftLogs.getCurrentTermIndex());
        assertTrue(raftLogs.hasStartPointSnapshot());
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        assertFalse(raftLogs.containsStartPoint(new TermIndex(1, 1L)));
        assertTrue(raftLogs.getLogRange(TermIndex.EPOCH, 25).isEmpty());

        // Computed appends continue past the boundary, never into the compacted region
        final RaftLog<String> c = raftLogs.appendLog(1, "c");
        assertEquals(3L, c.getTermIndex().getIndex());

        // Committing up to, and past, the boundary re-applies nothing; fresh entries commit as usual
        assertTrue(raftLogs.commit(new TermIndex(1, 2L)).isEmpty());
        final List<RaftLog<?>> committed = raftLogs.commit(new TermIndex(1, 3L));
        assertEquals(1, committed.size());
        assertEquals("c", committed.get(0).getEntry());
    }

    @Test
    void testCompactGuards() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a");
        raftLogs.appendLog(1, "b");

        assertThrows(IllegalArgumentException.class, () -> raftLogs.compact(null, "payload"));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.compact(new TermIndex(1, -1L), "payload"));

        // (6r) The index half of the boundary axis is pinned above, the term half was not:
        // the TermIndex constructor forbids a negative term, but a construction-bypassed
        // carrier (a deserialized or reflected one -- the 6(n)/6(p) mechanism) carries one.
        // Accepted, it reseeds the deque and, full-install, becomes the recorded tail and
        // the start point: a term no TermIndex may legally hold, served back through
        // getStartTermIndex() onto the wire (the 6(g)-guarded install path refuses it
        // forever) and into the term-first compareTo ordering rollback and commit depend on.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.compact(forgeTerm(-1, 2L), "hostile"));
        // Refusal is zero-state-change: the start point did not move and the log is intact
        assertEquals(TermIndex.EPOCH, raftLogs.getStartTermIndex());
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 1L)));
        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());

        raftLogs.commit(new TermIndex(1, 1L));
        raftLogs.compact(new TermIndex(1, 1L), "payload1");

        // Same boundary again: idempotent, only the payload is updated
        raftLogs.compact(new TermIndex(1, 1L), "payload1b");
        assertEquals("payload1b", raftLogs.getSnapshotPayload());

        // S5: the start point only ever moves forward
        assertThrows(IllegalStateException.class, () -> raftLogs.compact(TermIndex.EPOCH, "payload2"));

        // Never drop locally-committed entries
        raftLogs.appendLog(1, "c");
        raftLogs.commit(new TermIndex(1, 2L));
        assertThrows(IllegalStateException.class, () -> raftLogs.compact(new TermIndex(2, 1L), "payload2"));
    }

    @Test
    void testCommitGuards() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        final RaftLog<String> a = raftLogs.appendLog(1, "a"); // (1,1)
        final RaftLog<String> b = raftLogs.appendLog(1, "b"); // (1,2)
        raftLogs.commit(new TermIndex(1, 1L)); // "a" committed, "b" pending

        // (6r) A null or negative-term commit pointer is hostile by construction (the
        // same family as the compact guards above): a null derefs to NPE at the index
        // bounds, and a negative term passes them both (they are index-only), sticking
        // into committedTermIndex and inverting the term-first compareTo the committed
        // filter and rollback rely on.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(null));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.commit(forgeTerm(-1, 1L)));

        // Refusal is zero-state-change: the committed pointer is untouched...
        assertEquals(new TermIndex(1, 1L), raftLogs.getCommittedTermIndex());
        assertTrue(raftLogs.containsStartPoint(b.getTermIndex()));
        // ...and the legitimate commit still returns exactly the pending entry. Had the
        // forged pointer stuck, its term-first ordering would have re-served the already-
        // committed "a" (double-apply) or dropped "b" entirely (an ack that never comes) --
        // breaking the apply-then-acknowledge ordering this method exists to protect.
        assertEquals(List.of(b), raftLogs.commit(new TermIndex(1, 2L)));
        assertEquals(new TermIndex(1, 2L), raftLogs.getCommittedTermIndex());
    }

    @Test
    void testAppendGuards() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a"); // (1,1)
        raftLogs.appendLog(1, "b"); // (1,2)

        // (6s) Same hostile-carrier guard family as the 6r compact/commit guards, at the
        // log's main content intake: the int overloads are construction-guarded (the
        // TermIndex constructor refuses both negative axes), but the TermIndex overloads
        // take the pointer raw, so a construction-bypassed carrier (the 6(n)/6(p) shape)
        // or a null arrives unguarded.
        // A null pointer derefs to NPE in the conflict walk (the log is never empty --
        // it is seeded with the epoch): the wrong family for a contract refusal.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendLog(null, "hostile"));
        // The same intake's future-returning overload: one mirror pin to hold the funnel.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendFutureLog(null, "hostile"));

        // A forged negative term is no higher than the epoch's, so it replaces nothing
        // and is never "before" an entry either under the term-first compareTo: it falls
        // out of the walk as an IllegalStateException "log gap" -- again the wrong family.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendLog(forgeTerm(-1, 1L), "hostile"));

        // A null entry is a contract refusal uniform in the pointer: at an existing one
        // the happy path silently returns the existing record (the entry is never read),
        // at a new one it NPEs out of the record construction.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendLog(new TermIndex(1, 1L), null));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendLog(new TermIndex(1, 9L), null));

        // Refusal is zero-state-change: every real record intact, the tail did not move,
        // and the legitimate sequence still resolves all the way to the end.
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 1L)));
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(List.of("a", "b"), raftLogs.getLogRange(new TermIndex(0, 0L), 2, RaftLog::getEntry));
        raftLogs.appendLog(1, "c"); // (1,3)
        assertEquals(3L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(List.of("a", "b", "c"), raftLogs.getLogRange(new TermIndex(0, 0L), 3, RaftLog::getEntry));
    }

    @Test
    void testAppendGuardsRefuseForgedNegativeIndex() {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        raftLogs.appendLog(1, "a"); // (1,1)
        raftLogs.appendLog(1, "b"); // (1,2)

        // (6s) The one hostile shape that mutates: a forged negative index "replaces"
        // the epoch seed under isReplacementOf (1 > 0 and -1 <= 0), so the conflict walk
        // sweeps the ENTIRE log and lodges the forged record below the true tail -- all
        // before any check in the current code, handing the caller a completed future for
        // index -1 while the next computed append would continue from index 0. The guard
        // must fire before the iterator touches the deque.
        assertThrows(IllegalArgumentException.class, () -> raftLogs.appendLog(forgeIndex(-1L), "hostile"));

        // Zero-state-change: both real records intact and the tail unmoved -- had the
        // sweep run first (pre-fix), the log would hold only the forged (1,-1) and every
        // one of these would fail.
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 1L)));
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        assertEquals(2L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(List.of("a", "b"), raftLogs.getLogRange(new TermIndex(0, 0L), 2, RaftLog::getEntry));
        raftLogs.appendLog(1, "c"); // (1,3) still lands exactly one past the tail
        assertEquals(3L, raftLogs.getCurrentTermIndex().getIndex());
    }

    @Test
    void testReadNotificationGuards() throws Exception {
        final RaftLogs raftLogs = new RaftLogs(mock(EventBus.class));
        final RaftLog<String> log1 = raftLogs.appendLog(1, "a"); // (1,1)
        raftLogs.appendLog(1, "b"); // (1,2)

        // (6t) The 6r/6s guard family, extended to the log's remaining public read and
        // notification surfaces: containsStartPoint, getLogRange and notifyCommitted.
        // The pointer is legitimate iff non-null with both axes non-negative; the list
        // is legitimate iff it holds no nulls.
        // Pre-fix shapes: a null pointer NPEs (at the startSeed equality check, or
        // inside the getLogRange stream filter), a null transform NPEs at the map,
        // forged negative axes are silently answered "not contained" or serve a wider
        // range than asked, and notifyCommitted(null) NPEs in the for-each while a null
        // element is skipped without any trace (the instanceof test is simply false) --
        // the caller cannot tell a contract refusal apart from a skipped acknowledgement.

        // containsStartPoint: the null pointer and both forged negative axes are contract
        // refusals, not an NPE or a quiet "no"...
        assertThrows(IllegalArgumentException.class, () -> raftLogs.containsStartPoint(null));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.containsStartPoint(forgeTerm(-1, 1L)));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.containsStartPoint(forgeIndex(-1L)));
        // ...while legitimate pointers still resolve, and unknown ones still answer false.
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 1L)));
        assertTrue(raftLogs.containsStartPoint(new TermIndex(1, 2L)));
        assertFalse(raftLogs.containsStartPoint(new TermIndex(9, 9L)));

        // getLogRange: the two-arg overload funnels into the three-arg form, so the gate
        // at the three-arg version holds both; null pointer, forged axes and a null
        // transform are all refusals...
        assertThrows(IllegalArgumentException.class, () -> raftLogs.getLogRange(null, 2));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.getLogRange(null, 2, RaftLog::getEntry));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.getLogRange(forgeTerm(-1, 1L), 10, RaftLog::getEntry));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.getLogRange(forgeIndex(-1L), 10, RaftLog::getEntry));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.getLogRange(new TermIndex(0, 0L), 2, null));
        // ...while a legitimate range is served exactly.
        assertEquals(List.of("a", "b"), raftLogs.getLogRange(new TermIndex(0, 0L), 2, RaftLog::getEntry));

        // notifyCommitted: a null list and null elements are refusals, whether the list
        // is empty, null-only, or mixed -- the mixed one must not complete the good
        // record before refusing the bad neighbour (pre-fix it did, silently).
        assertThrows(IllegalArgumentException.class, () -> raftLogs.notifyCommitted(null));
        assertThrows(IllegalArgumentException.class, () -> raftLogs.notifyCommitted(Collections.singletonList(null)));
        final List<RaftLog<?>> mixed = new ArrayList<>();
        mixed.add(log1);
        mixed.add(null);
        assertThrows(IllegalArgumentException.class, () -> raftLogs.notifyCommitted(mixed));
        // A legitimate acknowledgement still completes a waiting client.
        final CompletableFuture<RaftLog<String>> pending = raftLogs.appendFutureLog(1, "c"); // (1,3)
        final List<RaftLog<?>> newlyCommitted = raftLogs.commit(new TermIndex(1, 3L));
        assertFalse(newlyCommitted.isEmpty());
        raftLogs.notifyCommitted(newlyCommitted);
        assertEquals("c", pending.get(1, TimeUnit.SECONDS).getEntry());

        // Zero-state-change: none of the refusals touched a record -- the tail is the
        // legitimately appended one and the range is intact.
        assertEquals(3L, raftLogs.getCurrentTermIndex().getIndex());
        assertEquals(List.of("a", "b", "c"), raftLogs.getLogRange(new TermIndex(0, 0L), 3, RaftLog::getEntry));
    }

    /**
     * Forge a pointer the {@link TermIndex} constructor would never produce (a negative
     * term): build a legal one, then set the field directly -- the exact shape a
     * deserialized or otherwise construction-bypassed carrier would reach the log with
     * (the 6(n)/6(p) mechanism, on the 6(g) term axis).
     */
    private static TermIndex forgeTerm(int term, long index) {
        final TermIndex base = new TermIndex(0, index);
        try {
            final java.lang.reflect.Field field = TermIndex.class.getDeclaredField("term");
            field.setAccessible(true);
            field.set(base, term);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the pointer term", e);
        }
    }

    /**
     * Forge a pointer the {@link TermIndex} constructor would never produce (a negative
     * index): the same construction-bypass shape as {@link #forgeTerm(int, long)} on the
     * index axis -- the carrier the append intake must refuse no matter how it arrived.
     */
    private static TermIndex forgeIndex(long index) {
        final TermIndex base = new TermIndex(1, 0L);
        try {
            final java.lang.reflect.Field field = TermIndex.class.getDeclaredField("index");
            field.setAccessible(true);
            field.set(base, index);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the pointer index", e);
        }
    }
}