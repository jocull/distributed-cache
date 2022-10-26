package com.codefromjames.com.lib.raft;

import java.util.*;
import java.util.stream.Collectors;

public class RaftLogs {
    private long currentIndex = 0L;
    private long commitIndex = 0L;
    private final Deque<RaftLog<?>> logs = new ArrayDeque<>(); // TODO: Is this the right data structure?

    public synchronized long getCurrentIndex() {
        return currentIndex;
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized <T> RaftLog<T> appendLog(T rawLog) {
        final RaftLog<T> raftLog = new RaftLog<>(++currentIndex, rawLog);
        logs.add(raftLog);
        return raftLog;
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
