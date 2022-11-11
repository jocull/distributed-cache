package com.github.jocull.raftcache.lib.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

class RaftLogs {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftLogs.class);
    private static final CompletableRaftLog<?> EPOCH_LOG = new CompletableRaftLog<>(TermIndex.EPOCH, RaftLogs.class);

    private TermIndex currentTermIndex = TermIndex.EPOCH;
    private TermIndex committedTermIndex = TermIndex.EPOCH;
    private final Deque<CompletableRaftLog<?>> logs = new ArrayDeque<>(); // TODO: Is this the right data structure?

    public RaftLogs() {
        // Adding the epoch log as a start point simplifies operations checking indexes against a start point
        logs.add(EPOCH_LOG);
    }

    public synchronized TermIndex getCurrentTermIndex() {
        return currentTermIndex;
    }

    public synchronized TermIndex getCommittedTermIndex() {
        return committedTermIndex;
    }

    public synchronized boolean containsStartPoint(TermIndex index) {
        // TODO: Could probably be optimized by using actual offsets.
        //       Could track the "offset of offsets" to keep things
        //       consistent even after a snapshot.
        //
        // TODO: It would probably be faster to walk this backwards and check from the end?
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
        return appendLogInternal(new TermIndex(term, currentTermIndex.getIndex() + 1), rawLog);
    }

    private <T> CompletableRaftLog<T> appendLogInternal(TermIndex termIndex, T rawLog) {
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
        return raftLog;
    }

    public synchronized List<RaftLog<?>> getLogRange(TermIndex startTermIndex, int limit) {
        return getLogRange(startTermIndex, limit, Function.identity());
    }

    public synchronized <TOut> List<TOut> getLogRange(TermIndex startTermIndex, int limit, Function<RaftLog<?>, TOut> fnTransform) {
        return logs.stream()
                .filter(r -> r.getTermIndex().compareTo(startTermIndex) > 0)
                .map(fnTransform)
                .limit(limit)
                .collect(Collectors.toList());
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
            if (log.getTermIndex().compareTo(committedTermIndex) > 0) {
                removed.add(log);
                log.rollback();
                iterator.remove();
            }
        }
        return removed;
    }

    public synchronized List<RaftLog<?>> commit(TermIndex termIndex) {
        if (currentTermIndex.compareTo(termIndex) < 0) {
            throw new IllegalArgumentException("Cannot commit " + termIndex + " when current is " + currentTermIndex);
        }
        if (termIndex.compareTo(committedTermIndex) < 0) {
            throw new IllegalArgumentException("Cannot commit " + termIndex + " when committed is " + committedTermIndex);
        }

        // Move the commit index forwards and return the committed logs
        final TermIndex previousCommitIndex = committedTermIndex;
        committedTermIndex = termIndex;
        return logs.stream()
                .filter(r -> r.getTermIndex().compareTo(previousCommitIndex) > 0
                        && r.getTermIndex().compareTo(termIndex) <= 0)
                .peek(CompletableRaftLog::commit)
                .collect(Collectors.toList());
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
