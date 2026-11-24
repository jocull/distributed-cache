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

    @Test
    void aBoundaryWhoseTermIsNegative_isRejectedAndStoresNothing() {
        // (6p) The save() guard checked boundary.getIndex() < 0 but never boundary.getTerm() < 0.
        // A negative term is unreachable in normal construction (the TermIndex constructor rejects
        // BOTH axes — the 6(g) root mechanism), which made the axis look unguarded-but-harmless,
        // exactly as the null-payload construction guard looked unreachable in 6(n). A forged or
        // deserialized boundary (fields set without the constructor) reaches save() with term < 0,
        // and before the fix it was stored and handed back by loadLatest — a Stored naming a
        // boundary no state machine can have. Refuse at the argument guard, the shape of the
        // sibling index guard: before the slot write.
        final InMemorySnapshotStore store = new InMemorySnapshotStore();
        final TermIndex forged = forgeTerm(-3, 10L);

        assertThrows(IllegalArgumentException.class, () -> store.save(forged, PAYLOAD_A),
                "a negative term is undefined and must not be stored");
        assertTrue(store.loadLatest().isEmpty(), "a rejected save stores nothing");
    }

    @Test
    void aStoredWhoseBoundaryTermIsNegative_isRejectedByTheRecordItself() {
        // (6p) The shared SnapshotStore.Stored record is the store-independent line of defense.
        // It already refused a negative INDEX (and null, and — after 6(o) — an empty payload),
        // but not a negative TERM: a forged boundary slipped through a store save and was only
        // caught if that store remembered to check. The record must name only a boundary the
        // TermIndex constructor could produce, so the invariant holds even for a store that
        // forgets the term axis. Refusing at the record is defense-in-depth one layer up from
        // the save() guard; the save() guard is the behavioral fix (refuse before any write).
        final TermIndex forged = forgeTerm(-3, 10L);

        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotStore.Stored(forged, PAYLOAD_A),
                "the record must not name a boundary that is structurally invalid (a negative term)");
    }

    /**
     * Forge a boundary the {@link TermIndex} constructor would never produce (a negative term):
     * build a legal one, then set the field directly — the exact shape a deserialized or
     * otherwise construction-bypassed carrier would reach a store with (the 6(n) mechanism,
     * applied to the 6(g) term axis).
     */
    private static TermIndex forgeTerm(int term, long index) {
        final TermIndex base = new TermIndex(0, index);
        try {
            final java.lang.reflect.Field field = TermIndex.class.getDeclaredField("term");
            field.setAccessible(true);
            field.set(base, term);
            return base;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to forge the boundary term", e);
        }
    }
}
