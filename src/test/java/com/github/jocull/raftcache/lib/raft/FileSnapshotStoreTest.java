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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void aBoundaryWhoseTermIsNegative_isRejectedWithoutTearingOutTheValidSnapshot(@TempDir Path dir) {
        // (6p) The save() argument guard checked boundary.getIndex() < 0 but never
        // boundary.getTerm() < 0 — the TermIndex constructor rejects BOTH negative axes
        // (the 6(g) root mechanism), which made the term axis look unreachable to this
        // guard, exactly as the null-payload construction guard looked unreachable in 6(n).
        // A forged/deserialized boundary (fields set without the constructor) reaches save()
        // with a negative term, and there: the write target would be raft-snapshot--3-10.snap —
        // a name parseBoundary can NEVER read back (the first dash is at position 0; parsing
        // the empty pre-dash substring throws) — while deleteSuperseded runs BEFORE the
        // unloadability is discoverable, tearing out the previous valid snapshot. A save that
        // "succeeds" and leaves the store EMPTY: data loss with no throw, and a directory
        // holding a file not even a foreign writer would leave. Refuse at the argument guard,
        // in the shape of the sibling index guard: before any temp file, any write, any delete.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);
        final TermIndex forged = forgeTerm(-3, 10L);

        assertThrows(IllegalArgumentException.class, () -> store.save(forged, PAYLOAD_B),
                "a negative term is undefined: the constructor rejects it, and a forged one must not be persisted");

        // THE DISTINGUISHING PART: the valid snapshot that was there BEFORE must still load —
        // the "drop every earlier snapshot" pass is part of the mutation a refusal must avoid.
        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow(
                () -> new AssertionError("a refused save must not have torn out the valid snapshot"));
        assertEquals(new TermIndex(5, 10L), stored.boundary(),
                "a rejected save must not delete the previously valid snapshot");
        assertArrayEquals(PAYLOAD_A, stored.payload(), "the surviving snapshot's bytes are intact");
        assertEquals(1, files(dir).size(), "a rejected save leaves no file behind — no unreadable --3-10 name, no .tmp");
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
    void aHollowSnapshot_namedForAHigherBoundary_isInvisibleToLoadLatest(@TempDir Path dir) {
        // (6o) The last hollow member of the "looks like a snapshot but is not" family
        // 6(c)/6(d) closed on the file surface: a valid NAME with an EMPTY body — e.g.
        // raft-snapshot-99-99.snap left in a shared directory by a crashed or foreign
        // writer (a save() that wrote it would have been rejected: an empty payload is a
        // caller error, so this shape can only come from outside this store). 6(d) made
        // an UNPARSEABLE name invisible; this makes a hollow body invisible the same
        // way — skipped with a warning, left in place — so max-by-boundary still recovers
        // the real snapshot instead of handing back a store() could never have produced.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        try {
            // Named for a HIGHER boundary than the real snapshot: if hollow files were
            // candidates, max-by-boundary would "recover" the zero-byte one and the real
            // snapshot would be masked — exactly the failure the 6(c) orphan-.tmp pin
            // closes on the name axis.
            Files.write(dir.resolve("raft-snapshot-99-99.snap"), new byte[0]);
        } catch (IOException e) {
            throw new IllegalStateException("failed to seed the hollow snapshot", e);
        }

        assertEquals(2, files(dir).size(), "sanity: the hollow file physically sits next to the real snapshot");
        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(5, 10L), stored.boundary(),
                "a hollow file is a no-snapshot, not a snapshot: the real one still wins max-by-boundary");
        assertArrayEquals(PAYLOAD_A, stored.payload(), "loadLatest returns the REAL snapshot's bytes, not an empty array");
        assertTrue(Files.exists(dir.resolve("raft-snapshot-99-99.snap")),
                "load is read-only: a file it does not understand is left in place (the 6(d) contract)");
    }

    @Test
    void aDirectoryWhoseOnlySnapshotIsHollow_loadsAsEmpty(@TempDir Path dir) {
        // (6o) With nothing to fall back behind, a hollow-only directory must read as "no
        // snapshot" — the same Optional.empty() a fresh store gives — not throw (the 6(d)
        // clean-load contract: an unknown file must not blow up an otherwise clean load)
        // and not hand back a Stored with an empty payload (a state save() rejects).
        try {
            Files.write(dir.resolve("raft-snapshot-3-7.snap"), new byte[0]);
        } catch (IOException e) {
            throw new IllegalStateException("failed to seed the hollow snapshot", e);
        }

        assertTrue(new FileSnapshotStore(dir).loadLatest().isEmpty(),
                "a zero-byte file is not a snapshot: the directory holds nothing loadable");
        assertTrue(Files.exists(dir.resolve("raft-snapshot-3-7.snap")),
                "load is read-only: the hollow file is left in place");
    }

    @Test
    void aDirectoryNamedLikeASnapshot_isInvisibleToLoadLatest(@TempDir Path dir) throws IOException {
        // (6q) The third member of the "looks like a snapshot but is not" family: 6(d) closed
        // the NAME axis (unparseable), 6(o) the BODY axis (zero bytes) — but the TYPE axis was
        // open. 6(o)'s hasPayload gate is a stat, and a directory's stat legitimately reports
        // a positive size, so a NON-REGULAR entry named raft-snapshot-99-99.snap passes every
        // gate there is: name shape, parseable boundary, "has payload". Max-by-boundary then
        // "recovers" the directory, the read throws, and the one VALID snapshot that sits
        // next to it is lost — a node seeded at startup would start unseeded over a log
        // already compacted to the real snapshot's boundary. A save() only ever writes
        // regular files, so this shape can only come from outside this store.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        // Named for a HIGHER boundary than the real snapshot: if non-regular entries were
        // candidates, max-by-boundary would "recover" the directory and the real snapshot
        // would be masked — the same shape as 6(d)'s orphan-.tmp and 6(o)'s hollow-file pins.
        Files.createDirectory(dir.resolve("raft-snapshot-99-99.snap"));

        assertTrue(Files.isDirectory(dir.resolve("raft-snapshot-99-99.snap")), "sanity: the impostor is a directory");
        assertEquals(1, files(dir).size(), "sanity: the directory sits next to exactly one real snapshot");
        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow(
                () -> new AssertionError("a non-regular entry must not blow up an otherwise clean load"));
        assertEquals(new TermIndex(5, 10L), stored.boundary(),
                "a directory is not a snapshot: the real one still wins max-by-boundary");
        assertArrayEquals(PAYLOAD_A, stored.payload(), "loadLatest returns the REAL snapshot's bytes");
        assertTrue(Files.isDirectory(dir.resolve("raft-snapshot-99-99.snap")),
                "load is read-only: an entry it does not read is left in place (the 6(d) contract)");
    }

    @Test
    void aDirectoryWhoseOnlySnapshotEntryIsNonRegular_loadsAsEmpty(@TempDir Path dir) throws IOException {
        // (6q) With nothing to fall back behind, a directory-only "store" must read as "no
        // snapshot" — the same Optional.empty() a fresh store gives (the 6(o) hollow-only
        // pin's twin on the type axis) — not throw, and not hand back a Stored whose payload
        // was read from a directory.
        Files.createDirectory(dir.resolve("raft-snapshot-99-99.snap"));

        assertTrue(new FileSnapshotStore(dir).loadLatest().isEmpty(),
                "a non-regular entry is not a snapshot: the directory holds nothing loadable");
        assertTrue(Files.isDirectory(dir.resolve("raft-snapshot-99-99.snap")),
                "load is read-only: the entry is left in place");
    }

    @Test
    void save_leavesANonRegularForeignEntryAlone_evenAtASupersededBoundary(@TempDir Path dir) throws IOException {
        // (6q) The write path half of the type axis: deleteSuperseded visited every
        // snapshot-named entry other than the new one, including NON-REGULAR foreign
        // entries. deleteIfExists on a NON-EMPTY directory throws — and that failure escapes
        // save() AFTER the new snapshot is already moved into place, so a partial save
        // reports as a failure (a RuntimeException, not even the same family a read failure
        // gives); on an EMPTY foreign directory it would instead silently tear down a
        // foreign object. Only regular files are snapshots: a save() writes regular files
        // and nothing else, so the retention pass must spare everything that is not one.
        final FileSnapshotStore store = new FileSnapshotStore(dir);
        store.save(new TermIndex(5, 10L), PAYLOAD_A);

        final Path foreign = dir.resolve("raft-snapshot-3-7.snap");
        Files.createDirectory(foreign); // a directory, at a LOWER boundary than what is about to be saved
        Files.write(foreign.resolve("foreign-content"), new byte[]{0x42});

        store.save(new TermIndex(9, 20L), PAYLOAD_B); // must complete, not throw on the foreign entry

        final SnapshotStore.Stored stored = store.loadLatest().orElseThrow();
        assertEquals(new TermIndex(9, 20L), stored.boundary(), "the new snapshot is the latest");
        assertArrayEquals(PAYLOAD_B, stored.payload());
        assertTrue(Files.isDirectory(foreign),
                "a non-regular entry is not a snapshot: the retention pass does not tear it out");
        assertArrayEquals(new byte[]{0x42}, Files.readAllBytes(foreign.resolve("foreign-content")),
                "a store never reads or deletes foreign contents it does not own");
        final Path superseded = dir.resolve("raft-snapshot-5-10.snap");
        assertFalse(Files.exists(superseded),
                "the SUPERSEDED REGULAR snapshot is still deleted: only the foreign entry is spared");
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
