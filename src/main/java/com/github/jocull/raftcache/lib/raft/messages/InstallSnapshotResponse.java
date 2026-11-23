package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;

/**
 * A follower's reply to {@link InstallSnapshot}.
 *
 * <p>The term lets the leader detect a higher-term peer (step down when
 * {@code response.getTerm() > request.getTerm()}); the index is the follower's
 * log tail (so the leader can set that follower's nextIndex past the snapshot
 * boundary and resume {@link AppendEntries}, SNAPSHOTS.md stages 3 and 5).
 *
 * <p>{@code offset} is the follower's authoritative byte cursor (SNAPSHOTS.md
 * stage 6a, etcd-style chunked transfer):
 * <ul>
 *   <li>on an <b>acceptance</b> (a chunk appended, or the final chunk installed)
 *       it is the <b>next expected byte offset</b> — the end of what the follower
 *       has now received, so the leader resumes exactly there;
 *   <li>on a <b>refusal</b> (term out of order, S2 progress check failed, or a
 *       chunk that does not continue the in-progress transfer) it is {@code 0},
 *       meaning "start over from byte 0".
 * </ul>
 * A legacy (pre-chunking) reply therefore reads as {@code offset=0} with the
 * installed tail as its index — still a coherent cursor for the leader.
 */
public class InstallSnapshotResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    // Current term, for the leader to update itself
    private final int term;
    // Log tail index after (or before, on refusal) the snapshot transfer
    private final long index;
    // Next expected byte offset (0 on refusal = "start over from byte 0")
    private final long offset;

    public InstallSnapshotResponse(int term, long index) {
        this(term, index, 0L);
    }

    public InstallSnapshotResponse(int term, long index, long offset) {
        this.term = term;
        this.index = index;
        this.offset = offset;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{" +
                "term=" + term +
                ", index=" + index +
                ", offset=" + offset +
                '}';
    }
}
