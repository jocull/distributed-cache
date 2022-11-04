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

    private long currentIndex = 0L;
    private long commitIndex = 0L;
    private final Deque<CompletableRaftLog<?>> logs = new ArrayDeque<>(); // TODO: Is this the right data structure?

    public synchronized long getCurrentIndex() {
        return currentIndex;
    }

    public synchronized boolean containsStartPoint(int term, long index) {
        if (logs.isEmpty()) {
            return currentIndex == index;
        }

        // TODO: Could probably be optimized by using actual offsets.
        //       Could track the "offset of offsets" to keep things
        //       consistent even after a snapshot.
        return logs.stream()
                .dropWhile(log -> log.getIndex() < index)
                .findFirst()
                .filter(log -> log.getTerm() == term)
                .isPresent();
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized <T> CompletableFuture<RaftLog<T>> appendFutureLog(int currentTerm, T entry) {
        return appendLogInternal(currentTerm, entry).getFuture();
    }

    public synchronized <T> CompletableFuture<RaftLog<T>> appendFutureLog(int currentTerm, long index, T entry) {
        return appendLogInternal(currentTerm, index, entry).getFuture();
    }

    public synchronized <T> RaftLog<T> appendLog(int term, T rawLog) {
        return appendLogInternal(term, rawLog);
    }

    public synchronized <T> RaftLog<T> appendLog(int term, long index, T rawLog) {
        return appendLogInternal(term, index, rawLog);
    }

    private <T> CompletableRaftLog<T> appendLogInternal(int term, T rawLog) {
        return appendLogInternal(term, currentIndex + 1, rawLog);
    }

    private <T> CompletableRaftLog<T> appendLogInternal(int term, long index, T rawLog) {
        final Iterator<CompletableRaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final CompletableRaftLog<?> next = iterator.next();
            if (next.getIndex() < index) {
                continue;
            }
            if (next.getIndex() != index) {
                // Shouldn't ever happen?
                throw new IllegalStateException("Unexpected log gap! " + next.getIndex() + " vs " + index);
            }
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it.
            if (next.getTerm() < term) {
                int count = 1;
                iterator.remove();
                while (iterator.hasNext()) {
                    iterator.next();
                    iterator.remove();
                    count++;
                }
                LOGGER.debug("Removed {} logs from term {} (instead of {})", count, next.getTerm(), term);
                break;
            }
            if (next.getIndex() == index) {
                LOGGER.trace("Existing log found from term {}, index {}", next.getTerm(), next.getIndex());
                //noinspection unchecked
                return (CompletableRaftLog<T>) next;
            }
        }

        LOGGER.trace("Added new log with term {}, index {}", term, index);
        final CompletableRaftLog<T> raftLog = new CompletableRaftLog<>(term, index, rawLog);
        logs.add(raftLog);
        if (index > currentIndex) {
            currentIndex = index;
        }

        return raftLog;
    }

    public synchronized List<RaftLog<?>> getLogRange(long startIndex, int limit) {
        return getLogRange(startIndex, limit, Function.identity());
    }

    public synchronized <TOut> List<TOut> getLogRange(long startIndex, int limit, Function<RaftLog<?>, TOut> fnTransform) {
        return logs.stream()
                .filter(r -> r.getIndex() > startIndex)
                .map(fnTransform)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Discard any uncommitted logs and return them back to the caller
     */
    public synchronized List<RaftLog<?>> rollback() {
        // Reset the current index back to the committed point
        currentIndex = commitIndex;

        final List<RaftLog<?>> removed = new ArrayList<>();
        final Iterator<CompletableRaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final CompletableRaftLog<?> log = iterator.next();
            if (log.getIndex() > commitIndex) {
                removed.add(log);
                log.rollback();
                iterator.remove();
            }
        }
        return removed;
    }

    public synchronized List<RaftLog<?>> commit(long index) {
        if (currentIndex < index) {
            throw new IllegalArgumentException("Cannot commit " + index + " when current is " + currentIndex);
        }
        if (index < commitIndex) {
            throw new IllegalArgumentException("Cannot commit " + index + " when committed is " + commitIndex);
        }

        // Move the commit index forwards and return the committed logs
        final long previousCommitIndex = commitIndex;
        commitIndex = index;
        return logs.stream()
                .filter(r -> r.getIndex() > previousCommitIndex && r.getIndex() <= index)
                .peek(CompletableRaftLog::commit)
                .collect(Collectors.toList());
    }

    private static class CompletableRaftLog<T> extends RaftLog<T> {
        private final CompletableFuture<RaftLog<T>> future;

        public CompletableRaftLog(int term, long index, T entry) {
            super(term, index, entry);
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
