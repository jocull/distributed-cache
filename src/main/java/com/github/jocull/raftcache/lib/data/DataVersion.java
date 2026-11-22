package com.github.jocull.raftcache.lib.data;

import java.util.Collections;
import java.util.Map;

/**
 * A value of a key at a point in the state machine's history.
 *
 * <p>Optionally carries an expiration instant (epoch millis). {@link #NO_EXPIRY} means the
 * entry never expires; a positive value means the entry is expired for any
 * {@code now >= expiresAtEpochMillis}.
 *
 * <p>Also the entry payload of a {@link Snapshot} -- the expiration instant is an
 * <em>absolute</em> deadline (not a relative TTL), so a captured value restores with
 * identical expiry semantics on a node whose clock differs.
 *
 * <p>Immutable: the data map is a detached copy.
 */
public class DataVersion {
    /** No expiration (never expires). */
    public static final long NO_EXPIRY = Long.MAX_VALUE;

    private final Map<String, byte[]> data;
    private final long version;
    private final long expiresAtEpochMillis;

    public DataVersion(Map<String, byte[]> data, long version) {
        this(data, version, NO_EXPIRY);
    }

    public DataVersion(Map<String, byte[]> data, long version, long expiresAtEpochMillis) {
        this.data = Collections.unmodifiableMap(data);
        this.version = version;
        this.expiresAtEpochMillis = expiresAtEpochMillis;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public long getVersion() {
        return version;
    }

    /** The absolute expiration instant (epoch millis); {@link #NO_EXPIRY} if none. */
    public long getExpiresAtEpochMillis() {
        return expiresAtEpochMillis;
    }

    /**
     * Whether the entry has expired at (or before) {@code nowEpochMillis}.
     */
    public boolean isExpired(long nowEpochMillis) {
        return expiresAtEpochMillis <= nowEpochMillis;
    }
}
