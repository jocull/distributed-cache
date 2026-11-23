package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the {@link InMemorySnapshotStore} contract (SNAPSHOTS.md stage 6b): the latest-only
 * slot, the exact boundary + wire bytes handed back, the defensive copies that keep a
 * call that later mutates its own array from corrupting the store, and the argument
 * validation that a node-level caller can rely on.
 */
class InMemorySnapshotStoreTest {

    private static final byte[] PAYLOAD_A = {1, 2, 3};
    private static final byte[] PAYLOAD_B = {9, 8, 7, 6};

    @Test
    void freshStore_loadsEmpty() {
        assertTrue(new InMemorySnapshotStore().loadLatest().isEmpty(),
                "a store that has never saved holds nothing");
    }

    @Test
    void savedSnapshot_isLoadedBackAsBoundaryPlusExactBytes() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();
        final TermIndex boundary = new TermIndex(5, 10L);

        store.save(boundary, PAYLOAD_A);
        final Optional<SnapshotStore.Stored> stored = store.loadLatest();

        assertTrue(stored.isPresent(), "a saved snapshot must be loadable");
        assertEquals(boundary, stored.get().boundary(), "the boundary is carried in the API, not recovered from the bytes");
        assertArrayEquals(PAYLOAD_A, stored.get().payload(), "the stored bytes are exactly the wire bytes");
    }

    @Test
    void aSecondSave_supersedesTheFirst() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();

        store.save(new TermIndex(5, 10L), PAYLOAD_A);
        store.save(new TermIndex(6, 20L), PAYLOAD_B);

        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(6, 20L), stored.boundary(), "only the latest snapshot is retained");
        assertArrayEquals(PAYLOAD_B, stored.payload());
    }

    @Test
    void savedPayload_isDefensivelyCopied() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();
        final byte[] caller = {1, 2, 3};

        store.save(new TermIndex(1, 1L), caller);
        caller[0] = 99; // the caller mutates its own buffer after saving

        assertArrayEquals(PAYLOAD_A, store.loadLatest().orElseThrow().payload(),
                "a caller mutating its own array after save must not corrupt the store");
    }

    @Test
    void nullArguments_areRejected() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();

        assertThrows(NullPointerException.class, () -> store.save(null, PAYLOAD_A),
                "a null boundary is undefined and must not be persisted");
        assertThrows(NullPointerException.class, () -> store.save(new TermIndex(1, 1L), null),
                "a null payload is undefined and must not be persisted");
        assertTrue(store.loadLatest().isEmpty(), "a rejected save stores nothing");
    }

    @Test
    void anEmptyPayload_isRejected() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();

        assertThrows(IllegalArgumentException.class, () -> store.save(new TermIndex(1, 1L), new byte[0]),
                "an empty (no-state) snapshot is a caller error: there is always at least the JDK header");
        assertTrue(store.loadLatest().isEmpty());
    }

    @Test
    void epochBoundary_isAlegitimateBoundary() {
        final InMemorySnapshotStore store = new InMemorySnapshotStore();

        store.save(new TermIndex(0, 0L), PAYLOAD_A);
        assertNotNull(store.loadLatest().orElseThrow(), "an (0,0) boundary round-trips like any other");
        assertEquals(new TermIndex(0, 0L), store.loadLatest().orElseThrow().boundary());
    }
}
