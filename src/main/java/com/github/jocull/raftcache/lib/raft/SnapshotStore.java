package com.github.jocull.raftcache.lib.raft;

import java.util.Optional;

/**
 * Durable storage for the Raft snapshot a node has installed (SNAPSHOTS.md stage 6b).
 *
 * <p>The Raft layer persists the <em>exact wire bytes</em> a snapshot carries on
 * {@code InstallSnapshot} — a state-machine capture JDK-serialized exactly once (the
 * leader serializes its own capture; the follower reuses the assembled bytes) — together
 * with the <em>log boundary</em> the snapshot reflects
 * ({@code lastIncludedTerm}/{@code lastIncludedIndex}, Raft §7). Persisting the boundary
 * alongside the bytes — rather than recovering it by decoding the payload — keeps the log
 * layer free of any state-machine type: decoding is the state machine's job, so the store
 * hand-backs an opaque {@code (TermIndex, byte[])} pair.
 *
 * <p>The same bytes the wire carries are what the node saves (a follower never
 * re-serializes; a leader reuses the one serialization it pinned for the wire). Save a
 * snapshot and the node later reloads its exact bytes, so the saved state is
 * byte-stable across a restart.
 *
 * <p>Only the <em>latest</em> snapshot is retained: on restart a node needs exactly one
 * re-anchor point (the furthest compacted boundary). Every later compaction supersedes
 * the earlier one, so {@link #save(TermIndex, byte[])} overwrites whatever was there and
 * {@link #loadLatest()} returns a single snapshot at most.
 *
 * <p>Saves are expected to be atomic: a node that crashes during a save must observe
 * either the fully new snapshot or the fully old one, never a torn write.
 *
 * <p>{@link RaftNodeImpl} consults the store in three places:
 * <ul>
 *   <li><b>on compaction</b> (leader): persisted before the log is trimmed, so a failed
 *       save never drops log entries;</li>
 *   <li><b>on install</b> (follower): persisted before the log is re-anchored;</li>
 *   <li><b>at startup</b> (before the follower begins): the latest snapshot is reloaded and
 *       re-adopted into the state machine, and the log compacted to its boundary.</li>
 * </ul>
 */
public interface SnapshotStore {

    /**
     * Persist the latest snapshot: the log boundary it re-anchors to, and its exact
     * wire bytes.
     *
     * <p>A later {@link #save(TermIndex, byte[])} supersedes this one (only the latest
     * is retained). Implementations must make the write durable and atomic — a crash
     * mid-save leaves either the previous snapshot or this one, never a partial one.
     *
     * @param boundary the start point (term, index) the log is compacted to; must not be
     *                 {@code null} and must have a non-negative index
     * @param payload  the JDK-serialized snapshot bytes; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code boundary} or {@code payload} is invalid
     * @throws RuntimeException if the write cannot be completed; implementations wrap
     *                          I/O failures (the Raft layer has no checked-exception seam
     *                          for this, and a failed save must propagate to the caller)
     */
    void save(TermIndex boundary, byte[] payload);

    /**
     * @return the most-recently saved snapshot (its boundary and exact wire bytes), or an
     *         empty {@link Optional} when no snapshot has been saved (or the store has been
     *         emptied).
     */
    Optional<Stored> loadLatest();

    /**
     * A persisted snapshot: the log boundary it re-anchors to, and the exact wire bytes
     * that boundary reflects — the same byte array {@link #save(TermIndex, byte[])} was
     * given, so round-trip is byte-for-byte identical.
     *
     * @param boundary the start point (term, index) the snapshot re-anchors the log to
     * @param payload  the JDK-serialized snapshot bytes
     */
    record Stored(TermIndex boundary, byte[] payload) {
        public Stored {
            if (boundary == null) {
                throw new IllegalArgumentException("Stored boundary must not be null");
            }
            if (payload == null) {
                throw new IllegalArgumentException("Stored payload must not be null");
            }
            if (boundary.getIndex() < 0) {
                throw new IllegalArgumentException("Stored boundary index must not be negative: " + boundary);
            }
        }
    }
}
