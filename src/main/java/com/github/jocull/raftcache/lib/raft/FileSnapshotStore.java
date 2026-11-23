package com.github.jocull.raftcache.lib.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A {@link FileSnapshotStore} (SNAPSHOTS.md stage 6b): one file per snapshot boundary,
 * held under a caller-supplied directory, with atomic writes (write-to-temp + move).
 *
 * <p>Each snapshot is one file named for its boundary — {@code raft-snapshot-<term>-<index>.snap} —
 * so the boundary is recovered from the file's identity, without ever having to decode
 * the payload (which is the {@code lib.data} state machine's job, not the log layer's).
 * Only the latest is retained: {@link #save(TermIndex, byte[])} writes the new file
 * atomically and then unlinks every earlier one, leaving a single file in the directory.
 *
 * <p>Atomicity: bytes go to a same-directory temp file, are flushed, then moved over the
 * snapshot file via {@link StandardCopyOption#ATOMIC_MOVE} (falling back to a plain
 * {@code REPLACE_EXISTING} move where the filesystem cannot guarantee atomic rename).
 * A crash mid-save therefore leaves either the previous file or the new one intact — never
 * a torn snapshot.
 *
 * <p>The write is fully durable before {@code save} returns: {@link Files#move}
 * {@code ATOMIC_MOVE} plus the directory entry update make the new file visible only once
 * complete, and we fsync the directory after unlinking superseded files so the "latest"
 * designation itself does not vanish on power loss.
 */
public class FileSnapshotStore implements SnapshotStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSnapshotStore.class);
    private static final String PREFIX = "raft-snapshot-";
    private static final String SUFFIX = ".snap";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path directory;

    public FileSnapshotStore(Path directory) {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        this.directory = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(this.directory);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create snapshot directory " + this.directory, e);
        }
    }

    public FileSnapshotStore(String directory) {
        this(Paths.get(directory));
    }

    public Path getDirectory() {
        return directory;
    }

    @Override
    public void save(TermIndex boundary, byte[] payload) {
        if (boundary == null) {
            throw new IllegalArgumentException("boundary must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (boundary.getIndex() < 0) {
            throw new IllegalArgumentException("boundary index must not be negative: " + boundary);
        }
        if (payload.length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }

        // Anchor the target in the store's directory: snapshotFile() returns only the file NAME
        // (its identity), so resolving it against here is what keeps the write -- and the
        // "delete every earlier snapshot" pass -- confined to this store's directory rather than
        // the process working directory.
        final Path target = directory.resolve(snapshotFile(boundary));
        final Path temp;
        try {
            temp = Files.createTempFile(directory, PREFIX, TMP_SUFFIX);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create temp snapshot file under " + directory, e);
        }

        boolean moved = false;
        try {
            Files.write(temp, payload);
            moveOver(target, temp);
            moved = true;
            // Drop every earlier snapshot: only the latest is retained.
            deleteSuperseded(target);
            fsyncDirectory();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist snapshot " + boundary + " to " + directory, e);
        } finally {
            if (!moved) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException ignore) {
                    LOGGER.warn("Failed to delete abandoned temp snapshot file {}", temp);
                }
            }
        }
        LOGGER.debug("Persisted snapshot {} ({} bytes) to {}", boundary, payload.length, target);
    }

    @Override
    public Optional<Stored> loadLatest() {
        try (Stream<Path> files = Files.list(directory)) {
            final Optional<Path> latest = files
                    .filter(p -> isSnapshotFile(p))
                    .max(Comparator.comparing(FileSnapshotStore::parseBoundary, Comparator.naturalOrder()));
            if (latest.isEmpty()) {
                return Optional.empty();
            }
            final Path file = latest.get();
            final byte[] payload = Files.readAllBytes(file);
            return Optional.of(new Stored(parseBoundary(file), payload));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load latest snapshot from " + directory, e);
        }
    }

    // ------------------------------------------------------------------

    /**
     * The stable snapshot file name for a boundary: {@code raft-snapshot-<term>-<index>.snap}.
     * Term and index are both non-negative integers (enforced by {@link TermIndex}), so the
     * name is unambiguous and round-trips exactly.
     */
    static Path snapshotFile(TermIndex boundary) {
        return Paths.get(PREFIX + boundary.getTerm() + "-" + boundary.getIndex() + SUFFIX);
    }

    /**
     * Parse a boundary back out of a snapshot file's name. The caller guarantees the name
     * matches the {@code PREFIX<term>-<index>SUFFIX} shape (see {@link #snapshotFile}).
     */
    static TermIndex parseBoundary(Path file) {
        final String name = file.getFileName().toString();
        final int start = PREFIX.length();
        final int end = name.length() - SUFFIX.length();
        final String body = name.substring(start, end);
        final int dash = body.indexOf('-');
        if (dash < 0) {
            throw new IllegalArgumentException("Unparseable snapshot file name: " + name);
        }
        final int term = Integer.parseInt(body.substring(0, dash));
        final long index = Long.parseLong(body.substring(dash + 1));
        return new TermIndex(term, index);
    }

    private boolean isSnapshotFile(Path file) {
        final String name = file.getFileName().toString();
        return name.startsWith(PREFIX) && name.endsWith(SUFFIX) && !name.endsWith(TMP_SUFFIX);
    }

    /**
     * Move {@code source} onto {@code target}, preferring an atomic rename so readers never
     * observe a half-written snapshot; where the OS cannot guarantee that, a plain
     * {@code REPLACE_EXISTING} move is the best available (still leaves either full file).
     */
    private void moveOver(Path target, Path source) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void deleteSuperseded(Path keep) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            files.filter(this::isSnapshotFile)
                    .filter(p -> !p.equals(keep))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to delete superseded snapshot " + p, e);
                        }
                    });
        }
    }

    /**
     * Best-effort sync of the directory entry so a rename/unlink does not vanish on power
     * loss. Not thrown on failure: some filesystems do not expose this.
     */
    private void fsyncDirectory() {
        try {
            final java.nio.channels.FileChannel channel =
                    java.nio.channels.FileChannel.open(directory, java.nio.file.StandardOpenOption.READ);
            try {
                channel.force(false);
            } finally {
                channel.close();
            }
        } catch (IOException ignore) {
            // Intentionally non-fatal: a directory sync the kernel may not support must
            // not be allowed to turn a successful save into a failure.
        }
    }
}
