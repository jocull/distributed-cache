package com.github.jocull.raftcache.lib.raft;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An in-memory {@link SnapshotStore} (SNAPSHOTS.md stage 6b): a single slot holding the
 * latest boundary + wire bytes.
 *
 * <p>Useful as the default for tests and for the production wiring until a durable
 * backing is chosen. It is <b>not</b> durable — a process restart loses the snapshot —
 * which is the whole reason {@link FileSnapshotStore} exists.
 *
 * <p>Only the latest snapshot is retained; each {@link #save(TermIndex, byte[])}
 * atomically replaces the slot. Concurrency-safe (a {@link AtomicReference} swap), so it
 * can be handed to a node from any thread.
 */
public class InMemorySnapshotStore implements SnapshotStore {

    private final AtomicReference<Stored> latest = new AtomicReference<>();

    @Override
    public void save(TermIndex boundary, byte[] payload) {
        Objects.requireNonNull(boundary, "boundary must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        if (boundary.getIndex() < 0) {
            throw new IllegalArgumentException("boundary index must not be negative: " + boundary);
        }
        if (payload.length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        // Copy so a caller mutating its own array after save cannot corrupt what we store.
        latest.set(new Stored(boundary, payload.clone()));
    }

    @Override
    public Optional<Stored> loadLatest() {
        return Optional.ofNullable(latest.get());
    }
}
