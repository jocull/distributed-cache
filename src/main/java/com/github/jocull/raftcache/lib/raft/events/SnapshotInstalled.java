package com.github.jocull.raftcache.lib.raft.events;

/**
 * A follower accepted an {@code InstallSnapshot} and re-anchored itself on the contained
 * snapshot (SNAPSHOTS.md stage 4).
 *
 * <p>Published on the node's own {@code EventBus} by the follower that accepted the
 * snapshot. The state-machine owner -- a {@code StateMachineBridge} subscribed to that
 * bus -- consumes the event and {@code adopt}s the payload, re-anchoring the applied
 * position behind the boundary so the replay guard skips exactly the entries the
 * snapshot already reflects.
 *
 * <p>{@code term}/{@code index} are the snapshot's own boundary
 * ({@code lastIncludedTerm}/{@code lastIncludedIndex}, Raft §7) -- the start point the
 * follower now records. {@code payload} is the portable state the boundary reflects
 * (a {@code com.github.jocull.raftcache.lib.data.Snapshot} in production): deliberately
 * typed as a loose {@link Object}, because this event crosses the {@code lib.raft} ->
 * {@code lib.data} boundary and the Raft layer must not name any state-machine type. A
 * subscriber that does not recognise the payload type simply ignores the event.
 */
public class SnapshotInstalled {
    private final int term;
    private final long index;
    private final Object payload;

    public SnapshotInstalled(int term, long index, Object payload) {
        this.term = term;
        this.index = index;
        this.payload = payload;
    }

    public int getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public Object getPayload() {
        return payload;
    }
}
