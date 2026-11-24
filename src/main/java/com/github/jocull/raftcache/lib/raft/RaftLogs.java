package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.raft.events.AppendLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A node's Raft log: an index-ordered deque of (term, index) -> entry records,
 * seeded by a start record.
 *
 * <p>The start record is a position marker, never content: the epoch for a fresh
 * log, or -- after {@link #compact} -- the snapshot boundary (SNAPSHOTS.md stage 2).
 * It is deliberately excluded from {@link #commit} and {@link #getLogRange} results:
 * the boundary's state is already reflected in the state machine (adopted via the
 * payload), so returning it for re-application would be a double-apply.
 */
class RaftLogs {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftLogs.class);
    private static final CompletableRaftLog<?> EPOCH_LOG = new CompletableRaftLog<>(TermIndex.EPOCH, RaftLogs.class);

    private final EventBus eventBus;
    private final Deque<CompletableRaftLog<?>> logs = new ArrayDeque<>(); // TODO: Is this the right data structure?

    private TermIndex currentTermIndex = TermIndex.EPOCH;
    private TermIndex committedTermIndex = TermIndex.EPOCH;

    // Snapshot boundary (SNAPSHOTS.md stage 2): the last-included (term, index) the log
    // was compacted to, held by the current start seed. Nullable == no snapshot yet: the
    // start point is the epoch and the payload is absent. The payload is the portable
    // state the boundary reflects (a com.github.jocull.raftcache.lib.data.Snapshot in
    // practice); a future InstallSnapshot message (stage 3) will carry it across the wire.
    private TermIndex snapshotBoundary = null;
    private Object snapshotPayload = null;
    private CompletableRaftLog<?> startSeed = EPOCH_LOG;

    public RaftLogs(EventBus eventBus) {
        this.eventBus = eventBus;

        // Adding the epoch log as a start point simplifies operations checking indexes against a start point
        logs.add(EPOCH_LOG);
    }

    public synchronized TermIndex getCurrentTermIndex() {
        return currentTail();
    }

    public synchronized TermIndex getCommittedTermIndex() {
        return committedTermIndex;
    }

    /**
     * The log's true tail (SNAPSHOTS.md S3): whichever is further <em>by index</em>,
     * the last recorded entry or the snapshot boundary. After a follower installs a
     * snapshot (stage 4) the recorded tail can sit behind the boundary, and everything
     * downstream (vote reporting, the nextIndex a leader computes, computed appends)
     * must treat the boundary as the last known entry.
     */
    private TermIndex currentTail() {
        if (snapshotBoundary != null && snapshotBoundary.getIndex() > currentTermIndex.getIndex()) {
            return snapshotBoundary;
        }
        return currentTermIndex;
    }

    public synchronized boolean containsStartPoint(TermIndex index) {
        // O(1) common cases (closes the old TODOs): the start point is an exact field
        // hit (epoch for a fresh log, the snapshot boundary after compact), and anything
        // before the start point is compacted away and can never match.
        if (index.equals(startSeed.getTermIndex())) {
            return true;
        }
        if (index.getIndex() < startSeed.getTermIndex().getIndex()) {
            return false;
        }
        return logs.stream().anyMatch(log -> log.getTermIndex().equals(index));
    }

    public synchronized <T> CompletableFuture<RaftLog<T>> appendFutureLog(int currentTerm, T entry) {
        return appendLogInternal(currentTerm, entry).getFuture();
    }

    public synchronized <T> CompletableFuture<RaftLog<T>> appendFutureLog(TermIndex termIndex, T entry) {
        return appendLogInternal(termIndex, entry).getFuture();
    }

    public synchronized <T> RaftLog<T> appendLog(int term, T rawLog) {
        return appendLogInternal(term, rawLog);
    }

    public synchronized <T> RaftLog<T> appendLog(TermIndex termIndex, T rawLog) {
        return appendLogInternal(termIndex, rawLog);
    }

    private <T> CompletableRaftLog<T> appendLogInternal(int term, T rawLog) {
        // Computed appends continue past the true tail: when the recorded tail lags
        // behind the snapshot boundary (post-install), appending off the raw field would
        // land inside the compacted region and break the append conflict walk.
        return appendLogInternal(new TermIndex(term, currentTail().getIndex() + 1), rawLog);
    }

    // TODO: Profiling shows bad performance in this section.
    //       Needs to be optimized as it gets much worse as log size grows.
    private <T> CompletableRaftLog<T> appendLogInternal(TermIndex termIndex, T rawLog) {
        // (6s) Same hostile-carrier guard family as the 6r compact/commit guards, at the
        // one append intake that is not construction-guarded: the int overloads route
        // through the TermIndex constructor (which refuses both negative axes), but this
        // overload takes the pointer raw, so a construction-bypassed carrier (the
        // 6(n)/6(p) shape) or a null arrives unguarded. A null pointer derefs to NPE in
        // the conflict walk (the log is never empty -- it is seeded with the epoch); a
        // forged negative term falls out as an ISE "log gap" (a negative term replaces
        // nothing and is never "before" an entry under the term-first compareTo); a
        // forged negative index is the worst shape -- it trips isReplacementOf against
        // the epoch seed (e.g. 1 > 0 and -1 <= 0) and the sweep destroys the ENTIRE log
        // before the forged record lodges below the true tail -- with a completed future
        // for the caller. A null entry, meanwhile, NPEs out of the record construction
        // at a new index and silently no-ops at an existing one. The gate refuses all of
        // them as contract violations, before the deque is touched.
        if (termIndex == null) {
            throw new IllegalArgumentException("append termIndex must not be null");
        }
        if (termIndex.getTerm() < 0 || termIndex.getIndex() < 0) {
            throw new IllegalArgumentException("append termIndex must not be negative: " + termIndex);
        }
        if (rawLog == null) {
            throw new IllegalArgumentException("append entry must not be null (pointer " + termIndex + ")");
        }
        final Iterator<CompletableRaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final CompletableRaftLog<?> next = iterator.next();
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it.
            if (termIndex.isReplacementOf(next.getTermIndex())) {
                int count = 1;
                iterator.remove();
                while (iterator.hasNext()) {
                    iterator.next();
                    iterator.remove();
                    count++;
                }
                LOGGER.debug("Removed {} logs from term {} (instead of {})", count, next.getTermIndex().getTerm(), termIndex);
                break;
            }
            if (next.getTermIndex().compareTo(termIndex) < 0) {
                continue;
            }
            if (!next.getTermIndex().equals(termIndex)) {
                // Shouldn't ever happen?
                throw new IllegalStateException("Unexpected log gap! " + next.getTermIndex() + " vs " + termIndex);
            }
            LOGGER.trace("Existing log found from @ {}", next.getTermIndex());
            //noinspection unchecked
            return (CompletableRaftLog<T>) next;
        }

        LOGGER.trace("Added new log @ {}", termIndex);
        final CompletableRaftLog<T> raftLog = new CompletableRaftLog<>(termIndex, rawLog);
        logs.add(raftLog);
        if (termIndex.compareTo(currentTermIndex) > 0) {
            currentTermIndex = termIndex;
        }
        eventBus.publish(new AppendLog(raftLog));
        return raftLog;
    }

    public synchronized List<RaftLog<?>> getLogRange(TermIndex startTermIndex, int limit) {
        return getLogRange(startTermIndex, limit, Function.identity());
    }

    public synchronized <TOut> List<TOut> getLogRange(TermIndex startTermIndex, int limit, Function<RaftLog<?>, TOut> fnTransform) {
        // Batched by the index axis, and never serves the start record (epoch, or the
        // snapshot boundary after compact): its state is already in the state machine
        // and re-applying it would be a double-apply.
        return logs.stream()
                .filter(r -> r != startSeed
                        && r.getTermIndex().getIndex() > startTermIndex.getIndex())
                .map(fnTransform)
                .limit(limit)
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------
    // Snapshot boundary (SNAPSHOTS.md stage 2)
    // ------------------------------------------------------------------

    /**
     * The start point of the log: the epoch for a fresh log, or the last-included
     * (term, index) the log was compacted to.
     */
    public synchronized TermIndex getStartTermIndex() {
        return snapshotBoundary == null ? TermIndex.EPOCH : snapshotBoundary;
    }

    public synchronized boolean hasStartPointSnapshot() {
        return snapshotBoundary != null;
    }

    /**
     * The portable state the snapshot boundary reflects (a
     * {@code com.github.jocull.raftcache.lib.data.Snapshot} in practice), or {@code null}
     * on a fresh log. A stage-3 {@code InstallSnapshot} message will carry it across the wire.
     */
    public synchronized Object getSnapshotPayload() {
        return snapshotPayload;
    }

    /**
     * Compact the log to the boundary: drop every record at or below it, reseeding the
     * deque with the boundary record (exactly like {@link #EPOCH_LOG} but a real term/index).
     *
     * <p>Records above the boundary are kept, so the recorded tail and commit pointer are
     * left untouched (S1 in practice keeps the boundary at or below the committed index).
     * If <em>nothing</em> survives, this is a full install (SNAPSHOTS.md S2: the snapshot
     * sits beyond the local tail) and the boundary <em>becomes</em> the recorded tail.
     *
     * <p>S5: the start point only moves forward, so compacting backwards is refused;
     * compacting past a locally-committed index would drop adopted state and is refused
     * too.
     */
    public synchronized void compact(TermIndex boundary, Object payload) {
        if (boundary == null) {
            throw new IllegalArgumentException("compact boundary must not be null");
        }
        if (boundary.getTerm() < 0 || boundary.getIndex() < 0) {
            // (6r) The index half was guarded and the term half was not: a negative term is
            // one the TermIndex constructor forbids, reachable only through a
            // construction-bypassed carrier (a deserialized or reflected one -- the 6(n)/6(p)
            // mechanism). Accepted, the boundary would reseed the deque and, full-install,
            // become the recorded tail and the start point: a term no TermIndex may legally
            // hold, served back through getStartTermIndex() onto the wire (the 6(g)-guarded
            // install path refuses it forever) and into the term-first compareTo ordering
            // commit and rollback depend on.
            throw new IllegalArgumentException("compact boundary must not be negative: " + boundary);
        }
        if (boundary.getIndex() < startSeed.getTermIndex().getIndex()) {
            throw new IllegalStateException("Cannot compact to " + boundary
                    + "; the start point is already at " + startSeed.getTermIndex());
        }
        if (committedTermIndex.getIndex() > boundary.getIndex()) {
            throw new IllegalStateException("Cannot compact to " + boundary
                    + "; committed is already at " + committedTermIndex);
        }

        final CompletableRaftLog<?> boundarySeed = new CompletableRaftLog<>(boundary, RaftLogs.class);
        final Deque<CompletableRaftLog<?>> survivors = new ArrayDeque<>();
        survivors.add(boundarySeed);
        int dropped = 0;
        for (CompletableRaftLog<?> log : logs) {
            if (log == startSeed) {
                continue;
            }
            if (log.getTermIndex().getIndex() > boundary.getIndex()) {
                survivors.add(log);
            } else {
                dropped++;
            }
        }

        logs.clear();
        logs.addAll(survivors);
        startSeed = boundarySeed;
        snapshotBoundary = boundary;
        snapshotPayload = payload;
        if (survivors.size() == 1) {
            // Full install: everything the log held was replaced by the boundary, so the
            // boundary is now the last known entry (the recorded tail may have lagged or
            // pointed at uncommitted garbage that the snapshot supersedes).
            currentTermIndex = boundary;
        }
        LOGGER.debug("Compacted log to boundary {} (dropped {} record(s), {} kept)", boundary, dropped, survivors.size() - 1);
    }

    /**
     * Discard any uncommitted logs and return them back to the caller
     */
    public synchronized List<RaftLog<?>> rollback() {
        // Reset the current index back to the committed point
        currentTermIndex = committedTermIndex;

        final List<RaftLog<?>> removed = new ArrayList<>();
        final Iterator<CompletableRaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final CompletableRaftLog<?> log = iterator.next();
            if (log == startSeed) {
                // The start record (epoch or snapshot boundary) is a position marker, not
                // rollbackable content: S5 -- the start point only ever moves forward.
                continue;
            }
            if (log.getTermIndex().compareTo(committedTermIndex) > 0) {
                removed.add(log);
                log.rollback();
                iterator.remove();
            }
        }
        return removed;
    }

    /**
     * Move the commit index forward and return the newly-committed logs.
     *
     * <p>This deliberately does <em>not</em> acknowledge waiting clients: the caller must
     * apply the entries to the state machine first (i.e. publish {@code LogsCommitted})
     * and only then call {@link #notifyCommitted(List)}. That ordering is what guarantees
     * a client cannot observe its own write as acknowledged before it has taken effect.
     */
    public synchronized List<RaftLog<?>> commit(TermIndex termIndex) {
        // (6r) Same hostile-carrier guard family as compact below: the pointer must be
        // present and both axes non-negative. The index half was only implicitly closed by
        // the committed-lower-bound check further down, and the term half (and the null,
        // which NPE'd out of the first index bound) were wide open: a forged negative term
        // passes both index comparisons, sticks into committedTermIndex, and inverts the
        // term-first compareTo the committed filter and rollback rely on -- re-serving
        // committed entries (double-apply) or dropping pending ones (an ack that never
        // comes), breaking the apply-then-acknowledge ordering this method protects.
        if (termIndex == null) {
            throw new IllegalArgumentException("commit termIndex must not be null");
        }
        if (termIndex.getTerm() < 0 || termIndex.getIndex() < 0) {
            throw new IllegalArgumentException("commit termIndex must not be negative: " + termIndex);
        }
        // The commit index moves along the index axis and cannot outrun the true tail --
        // which, post-install (stage 4), is the boundary even when the recorded tail lags.
        final TermIndex tail = currentTail();
        if (termIndex.getIndex() > tail.getIndex()) {
            throw new IllegalArgumentException("Cannot commit " + termIndex + " when current is " + tail);
        }
        if (termIndex.getIndex() < committedTermIndex.getIndex()) {
            throw new IllegalArgumentException("Cannot commit " + termIndex + " when committed is " + committedTermIndex);
        }

        // Move the commit index forwards and return the committed logs, skipping the start
        // record: its state was already adopted and re-applying it would be a double-apply.
        final TermIndex previousCommitIndex = committedTermIndex;
        committedTermIndex = termIndex;
        return logs.stream()
                .filter(r -> r != startSeed
                        && r.getTermIndex().compareTo(previousCommitIndex) > 0
                        && r.getTermIndex().compareTo(termIndex) <= 0)
                .collect(Collectors.toList());
    }

    /**
     * Acknowledge the clients that have been waiting on the given committed logs: complete
     * their futures (on the common pool, as before).
     *
     * <p>Must be called <em>after</em> the entries have been applied to the state machine
     * (after {@code LogsCommitted} has been published), never at the same time as
     * {@link #commit(TermIndex)}. Logs that have no waiting client (follower-replicated
     * entries, rollback-cancelled ones) are passed through with no effect.
     */
    public synchronized void notifyCommitted(List<RaftLog<?>> committedLogs) {
        for (RaftLog<?> committedLog : committedLogs) {
            if (committedLog instanceof CompletableRaftLog) {
                ((CompletableRaftLog<?>) committedLog).commit();
            }
        }
    }

    private static class CompletableRaftLog<T> extends RaftLog<T> {
        private final CompletableFuture<RaftLog<T>> future;

        public CompletableRaftLog(TermIndex termIndex, T entry) {
            super(termIndex, entry);
            this.future = new CompletableFuture<>();
        }

        public CompletableFuture<RaftLog<T>> getFuture() {
            return future;
        }

        public void rollback() {
            ForkJoinPool.commonPool().execute(() -> future.cancel(false));
        }

        public void commit() {
            future.completeAsync(() -> this, ForkJoinPool.commonPool());
        }
    }
}
