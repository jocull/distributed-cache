package com.codefromjames.com.lib.raft.messages;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AppendEntries {
    // Leader's term
    private final int term;
    // Index of log immediately preceding new ones
    private final long previousLogIndex;
    // Leader's commit index
    private final long leaderCommitIndex;
    // Log entries to store (or empty for heartbeat)
    private final List<RaftLog> entries;

    // TODO: Needs more work, just a rough heartbeat right now to confirm leadership
    public AppendEntries(int term, long previousLogIndex, long leaderCommitIndex, List<RaftLog> entries) {
        this.term = term;
        this.previousLogIndex = previousLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = Collections.unmodifiableList(entries);
    }

    public int getTerm() {
        return term;
    }

    public long getPreviousLogIndex() {
        return previousLogIndex;
    }

    public long getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public List<RaftLog> getEntries() {
        return entries;
    }

    public static class RaftLog {
        private final long index;
        private final Object entry;

        public RaftLog(long index, Object entry) {
            Objects.requireNonNull(entry);

            this.index = index;
            this.entry = entry;
        }

        public long getIndex() {
            return index;
        }

        public Object getEntry() {
            return entry;
        }
    }
}
