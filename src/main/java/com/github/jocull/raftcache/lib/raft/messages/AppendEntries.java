package com.github.jocull.raftcache.lib.raft.messages;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AppendEntries {
    // Leader's term
    private final int term;
    // Index of log immediately preceding new ones
    private final TermIndex previousLogTermIndex;
    // Leader's commit index
    private final TermIndex leaderCommitTermIndex;
    // Log entries to store (or empty for heartbeat)
    private final List<RaftLog> entries;

    // TODO: Needs more work, just a rough heartbeat right now to confirm leadership
    public AppendEntries(int term, TermIndex previousLogTermIndex, TermIndex leaderCommitTermIndex, List<RaftLog> entries) {
        this.term = term;
        this.previousLogTermIndex = previousLogTermIndex;
        this.leaderCommitTermIndex = leaderCommitTermIndex;
        this.entries = Collections.unmodifiableList(entries);
    }

    public int getTerm() {
        return term;
    }

    public TermIndex getPreviousLogTermIndex() {
        return previousLogTermIndex;
    }

    public TermIndex getLeaderCommitTermIndex() {
        return leaderCommitTermIndex;
    }

    public List<RaftLog> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return "AppendEntries{" +
                "term=" + term +
                ", previousLogTermIndex=" + previousLogTermIndex +
                ", leaderCommitTermIndex=" + leaderCommitTermIndex +
                ", entries=" + entries +
                '}';
    }

    public static class RaftLog {
        private final TermIndex termIndex;
        private final Object entry;

        public RaftLog(TermIndex termIndex, Object entry) {
            Objects.requireNonNull(entry);

            this.termIndex = termIndex;
            this.entry = entry;
        }

        public TermIndex getTermIndex() {
            return termIndex;
        }

        public Object getEntry() {
            return entry;
        }

        @Override
        public String toString() {
            return "RaftLog{" +
                    "termIndex=" + termIndex +
                    ", entry=" + entry +
                    '}';
        }
    }
}
