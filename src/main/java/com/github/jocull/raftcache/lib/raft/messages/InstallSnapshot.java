package com.github.jocull.raftcache.lib.raft.messages;

import java.io.Serializable;
import java.util.Objects;

/**
 * A leader's request (Raft §7) for a follower that has fallen so far behind
 * that the leader no longer retains the log entries it needs: the follower
 * replaces its state machine and log with the contained snapshot.
 *
 * <p>SNAPSHOTS.md stage 3: single-chunk form; {@code offset} and {@code done}
 * are reserved for a chunked send (the {@code InstallSnapshotExample} sketch
 * shape, with the payload typed as a loose {@code Object} — a state-machine
 * snapshot known to the state machine, {@code Serializable} for the wire —
 * mirroring {@link AppendEntries.RaftLog#getEntry()}).
 *
 * <p>{@code lastIncludedIndex}/{@code lastIncludedTerm} (Raft §7) is exactly the
 * snapshot boundary a follower records as its new start point on acceptance.
 */
public class InstallSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    // Leader's term
    private final int term;
    // Leader's node id, so the follower can redirect clients
    private final String leaderId;
    // Log index the snapshot replaces everything up through and including (new start point)
    private final long lastIncludedIndex;
    // Term of the entry at lastIncludedIndex
    private final int lastIncludedTerm;
    // Byte offset where a chunk starts in the snapshot (reserved; 0 for a single chunk)
    private final long offset;
    // The snapshot payload: a state-machine snapshot object the receiver's state machine understands
    private final Object payload;
    // True if this is the last chunk (always true for a single chunk)
    private final boolean done;

    public InstallSnapshot(int term, String leaderId, long lastIncludedIndex, int lastIncludedTerm,
                           long offset, Object payload, boolean done) {
        this.term = term;
        this.leaderId = Objects.requireNonNull(leaderId);
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.payload = Objects.requireNonNull(payload);
        this.done = done;
    }

    public int getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long getOffset() {
        return offset;
    }

    public Object getPayload() {
        return payload;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", offset=" + offset +
                ", payload=" + payload +
                ", done=" + done +
                '}';
    }
}
