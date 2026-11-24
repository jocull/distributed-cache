package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
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
    void orphanTempFiles_areInvisibleToLoadLatest(@TempDir Path dir) {
        // A save that dies mid-write (the process crashes between temp-file creation and the
        // atomic move) leaves a .tmp behind in the directory: loadLatest must never read it
        // as a snapshot, whatever boundary it would nominally claim.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        try {
            // Deliberately name the orphan for a HIGHER boundary than the real snapshot: if
            // .tmp files were parsed, max-by-boundary would "recover" it and this test fails.
            Files.write(dir.resolve("raft-snapshot-99-99.tmp"), new byte[]{0x01, 0x02});
        } catch (IOException e) {
            throw new IllegalStateException("failed to simulate an orphan .tmp", e);
        }

        assertEquals(2, files(dir).size(), "sanity: the orphan physically exists next to the real snapshot");
        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(5, 10L), stored.boundary(), "a .tmp file is not a snapshot, even when named for a later boundary");
        assertArrayEquals(PAYLOAD_A, stored.payload(), "loadLatest returns the real snapshot's bytes");
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

    @Test
    void aSnapshotNameThatNamesNoBoundary_isSkippedAndTheValidOneStillLoads(@TempDir Path dir) {
        // (6d) The directory may hold a file that has a snapshot's NAME SHAPE but not a boundary —
        // e.g. raft-snapshot-foo.snap left by a crashed writer or a foreign tool working in a shared
        // directory. Before (6d) the parse failure inside the max-by-boundary comparator escaped
        // loadLatest and NO snapshot in the directory was recoverable; after, the valid snapshot
        // still loads and the stray file is left alone (recovery must be side-effect-free).
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        try {
            // Deliberately named for a HIGHER boundary than the real snapshot: if the loader
            // ever treated the stray as parseable, max-by-boundary would "recover" it.
            Files.write(dir.resolve("raft-snapshot-foo.snap"), new byte[]{0x01});
        } catch (IOException e) {
            throw new IllegalStateException("failed to seed the stray snapshot name", e);
        }

        assertEquals(2, files(dir).size(), "sanity: the stray physically sits next to the real snapshot");
        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(5, 10L), stored.boundary(),
                "an unparseable name is skipped, not allowed to blow up an otherwise clean load");
        assertArrayEquals(PAYLOAD_A, stored.payload(), "loadLatest returns the REAL snapshot's bytes");
        assertTrue(Files.exists(dir.resolve("raft-snapshot-foo.snap")),
                "load is read-only: a file it does not understand is left in place");
    }

    @Test
    void twoCoexistingValidSnapshots_loadLatestPicksTheHighestBoundary(@TempDir Path dir) {
        // (6d) save() deletes superseded files, so two valid files can only coexist if one was
        // written by something other than this save pass (a crashed mid-rename, a manual seed).
        // loadLatest must pick strictly by boundary, term-major then index (TermIndex ordering):
        // (1,5) versus (2,3) — an index-first or first-come comparison would recover the WRONG one.
        try {
            Files.write(dir.resolve("raft-snapshot-1-5.snap"), PAYLOAD_A);
            Files.write(dir.resolve("raft-snapshot-2-3.snap"), PAYLOAD_B);
        } catch (IOException e) {
            throw new IllegalStateException("failed to seed two snapshots", e);
        }

        assertEquals(2, files(dir).size(), "sanity: both files coexist");
        final SnapshotStore.Stored stored = new FileSnapshotStore(dir).loadLatest().orElseThrow();
        assertEquals(new TermIndex(2, 3L), stored.boundary(),
                "the highest boundary wins: term-major, so (2,3) beats (1,5) despite the lower index");
        assertArrayEquals(PAYLOAD_B, stored.payload(), "the winner is the file the winner is named for");
    }

    @Test
    void saveIntoAReadOnlyDirectory_failsLoudlyAndLeavesNoTempFile(@TempDir Path dir) throws IOException {
        // (6d) A save can fail MID-WRITE (a revoked permission, a disk fulling, a read-only mount
        // landing mid-lifetime). Two contracts the node relies on: the failure is LOUD (the
        // leader's compaction gate is fail-closed on exactly this), and the temp-file cleanup
        // still runs — no stray raft-snapshot-*.snap.tmp left in the directory. Note the read-only
        // premise does not hold for root (POSIX permissions are bypassed); the probe below turns
        // that into a clean skip instead of a false failure. Non-POSIX platforms (Windows) have no
        // NIO permission bit to clear at all: setPosixFilePermissions throws UnsupportedOperationException,
        // so the platform gate probes the view first and turns that into the same clean skip.
        final PosixFileAttributeView attributeView =
                Files.getFileAttributeView(dir, PosixFileAttributeView.class);
        Assumptions.assumeTrue(attributeView != null,
                "no POSIX file attribute view (non-POSIX platform); a read-only directory cannot be constructed");

        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        try {
            setPermissions(attributeView, false);
            boolean readOnly = true;
            try {
                final Path probe = Files.createTempFile(dir, "probe", ".txt");
                Files.deleteIfExists(probe);
                readOnly = false;
            } catch (IOException expected) {
                // good: this user really cannot write here
            }
            Assumptions.assumeTrue(readOnly, "this user can write a read-only directory (root); the premise does not hold");

            assertThrows(IllegalStateException.class,
                    () -> store.save(new TermIndex(6, 20L), PAYLOAD_B),
                    "a failed write is a loud error the caller can fail-closed on, not a silent drop");

            final List<Path> survivors = files(dir);
            assertEquals(1, survivors.size(), "a failed save leaves the last good snapshot exactly where it was");
            assertTrue(survivors.stream().noneMatch(p -> p.getFileName().toString().endsWith(".tmp")),
                    "a failed save leaves no temp file behind (the cleanup pass ran)");

            final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
            assertEquals(new TermIndex(5, 10L), stored.boundary(), "loadLatest returns the last successful one");
            assertArrayEquals(PAYLOAD_A, stored.payload());
        } finally {
            setPermissions(attributeView, true);
        }
    }

    private static void setPermissions(PosixFileAttributeView attributeView, boolean writable) throws IOException {
        // Owner bits are what matter: the test process is the owner. Keep the EXECUTE bit
        // both ways — POSIX directories need it for traversal (listing, deleting, path
        // resolution), and a directory that lost it breaks even JUnit's temp-dir cleanup.
        // The WRITE bit is the only thing toggled (execute stays set for both): the
        // 9-character POSIX form, writable "rwxrwxrwx", read-only "r-xr-xr-x". A null view
        // never reaches here (the platform gate already skipped the test).
        attributeView.setPermissions(PosixFilePermissions.fromString(writable ? "rwxrwxrwx" : "r-xr-xr-x"));
    }
}
