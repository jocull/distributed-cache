package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;

/**
 * A follower's reply to {@link InstallSnapshot}.
 *
 * <p>The term lets the leader detect a higher-term peer (step down when
 * {@code response.getTerm() > request.getTerm()}); the index is the follower's
 * log tail after installing the snapshot, so the leader can set that follower's
 * nextIndex past the snapshot boundary and resume {@link AppendEntries}
 * (SNAPSHOTS.md stages 3 and 5).
 */
public class InstallSnapshotResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    // Current term, for the leader to update itself
    private final int term;
    // Log tail index after installing the snapshot
    private final long index;

    public InstallSnapshotResponse(int term, long index) {
        this.term = term;
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{" +
                "term=" + term +
                ", index=" + index +
                '}';
    }
}
