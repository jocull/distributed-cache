package com.codefromjames.com.lib.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RaftLogs {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftLogs.class);

    private long currentIndex = 0L;
    private long commitIndex = 0L;
    private final Deque<RaftLog<?>> logs = new ArrayDeque<>(); // TODO: Is this the right data structure?

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

    public synchronized <T> RaftLog<T> appendLog(int term, T rawLog) {
        return appendLog(term, currentIndex + 1, rawLog);
    }

    public synchronized <T> RaftLog<T> appendLog(int term, long index, T rawLog) {
        final Iterator<RaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final RaftLog<?> next = iterator.next();
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
                return (RaftLog<T>) next;
            }
        }

        LOGGER.trace("Added new log with term {}, index {}", term, index);
        final RaftLog<T> raftLog = new RaftLog<>(term, index, rawLog);
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
        final Iterator<RaftLog<?>> iterator = logs.iterator();
        while (iterator.hasNext()) {
            final RaftLog<?> log = iterator.next();
            if (log.getIndex() > commitIndex) {
                removed.add(log);
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
        final List<RaftLog<?>> committed = logs.stream()
                .filter(r -> r.getIndex() > commitIndex && r.getIndex() <= index)
                .collect(Collectors.toList());
        commitIndex = index;
        return committed;
    }
}
