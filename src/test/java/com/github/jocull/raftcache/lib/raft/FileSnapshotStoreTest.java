package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the {@link FileSnapshotStore} contract (SNAPSHOTS.md stage 6b): one file per boundary,
 * named for it so the boundary is recovered without decoding, only the latest retained, and the
 * argument validation a node-level caller relies on. The directory is a JUnit {@code @TempDir}
 * (auto-cleaned), so the tests never touch a shared location.
 */
class FileSnapshotStoreTest {

    private static final byte[] PAYLOAD_A = {1, 2, 3};
    private static final byte[] PAYLOAD_B = {9, 8, 7, 6};

    private static List<Path> files(Path dir) {
        try (var list = Files.list(dir)) {
            return list.filter(Files::isRegularFile).toList();
        } catch (IOException e) {
            throw new IllegalStateException("failed to list " + dir, e);
        }
    }

    @Test
    void freshStore_loadsEmpty(@TempDir Path dir) {
        assertTrue(new FileSnapshotStore(dir).loadLatest().isEmpty(), "a store that has never saved holds nothing");
    }

    @Test
    void savedSnapshot_isRecoveredByItsNameAndExactBytes(@TempDir Path dir) {
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        final List<Path> files = files(dir);
        assertEquals(1, files.size(), "one snapshot is exactly one file");
        assertEquals("raft-snapshot-5-10.snap", files.get(0).getFileName().toString(),
                "the name encodes the boundary so it is recovered without decoding the payload");

        final Optional<SnapshotStore.Stored> stored = store.loadLatest();
        assertTrue(stored.isPresent(), "a saved snapshot must be loadable");
        assertEquals(new TermIndex(5, 10L), stored.get().boundary());
        assertArrayEquals(PAYLOAD_A, stored.get().payload(), "the stored bytes are exactly the wire bytes");
    }

    @Test
    void aSecondSave_retainsOnlyTheLatestFile(@TempDir Path dir) {
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);
        store.save(new TermIndex(6, 20L), PAYLOAD_B);

        final List<Path> files = files(dir);
        assertEquals(1, files.size(), "superseded snapshots are removed when a later one is saved");
        assertEquals("raft-snapshot-6-20.snap", files.get(0).getFileName().toString(),
                "the survivor is named for the latest boundary");

        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(6, 20L), stored.boundary(), "loadLatest returns the latest boundary");
        assertArrayEquals(PAYLOAD_B, stored.payload());
    }

    @Test
    void anOlderSnapshotIsDeleted_whenASmallerBoundaryIsSavedLater(@TempDir Path dir) {
        // Boundaries are not assumed to grow: the store is "latest save wins", keyed on file
        // identity, so saving a boundary LOWER than the previous one still leaves exactly one file.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(6, 20L), PAYLOAD_A);
        store.save(new TermIndex(5, 10L), PAYLOAD_B);

        final List<Path> files = files(dir);
        assertEquals(1, files.size(), "only the most recently SAVED snapshot is retained");
        assertEquals("raft-snapshot-5-10.snap", files.get(0).getFileName().toString());
    }

    @Test
    void epochBoundary_roundTrips(@TempDir Path dir) {
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(0, 0L), PAYLOAD_A);

        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(0, 0L), stored.boundary());
        assertArrayEquals(PAYLOAD_A, stored.payload());
    }

    @Test
    void nullOrEmptyArguments_areRejectedAndPersistNothing(@TempDir Path dir) {
        final FileSnapshotStore store = new FileSnapshotStore(dir);

        assertThrows(IllegalArgumentException.class, () -> store.save(null, PAYLOAD_A), "a null boundary is undefined");
        assertThrows(IllegalArgumentException.class, () -> store.save(new TermIndex(1, 1L), null), "a null payload is undefined");
        assertThrows(IllegalArgumentException.class, () -> store.save(new TermIndex(1, 1L), new byte[0]),
                "an empty (no-state) snapshot is a caller error");

        assertTrue(store.loadLatest().isEmpty(), "a rejected save stores nothing");
        assertTrue(files(dir).isEmpty(), "a rejected save leaves no file behind");
    }

    @Test
    void constructor_rejectsANullDirectory() {
        assertThrows(IllegalArgumentException.class, () -> new FileSnapshotStore((Path) null));
    }
}
