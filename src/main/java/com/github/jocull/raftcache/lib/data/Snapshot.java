package com.github.jocull.raftcache.lib.data;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An immutable state-machine snapshot: the complete key/value state captured at a
 * state-machine event, bound to the term and index of that event.
 *
 * <p>Entries carry the value's {@link DataVersion} -- version counter and absolute
 * expiration -- not a client-facing read view, so replaying a snapshot on another node
 * restores both the data and its remaining TTL, and subsequent writes continue the
 * version-counter sequence ({@link DataVersion#getVersion() versions are not the key's
 * value}; they order state-machine writes).
 *
 * <p>Stage 1 (SNAPSHOTS.md) is capture/replace only: no wire form. Stages 3-6 add the
 * {@code InstallSnapshot} message; the boundary (term, index) here is exactly what that
 * message carries (Raft §7: {@code lastIncludedIndex}/{@code lastIncludedTerm}).
 *
 * <p>The entries map is a detached copy; entries themselves ({@link DataVersion}) are
 * already immutable with a detached data map.
 *
 * @param term    term of the last log entry applied when the snapshot was taken (0 if none)
 * @param index   index of that entry, i.e. the last applied index (0 if none -- log indices start at 1)
 * @param entries key -> value with version and expiration, at the moment of capture
 */
public record Snapshot(long term, long index, Map<String, DataVersion> entries) implements Serializable {
    private static final long serialVersionUID = 1L;

    public Snapshot {
        if (entries == null) {
            throw new NullPointerException("entries must not be null");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0");
        }
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }

    /** The value (with version and expiration) captured for {@code key}, if any. */
    public DataVersion entry(String key) {
        return entries.get(key);
    }

    /** Number of keys captured. Zero is legal (a fully-compacted state machine). */
    public int size() {
        return entries.size();
    }
}
