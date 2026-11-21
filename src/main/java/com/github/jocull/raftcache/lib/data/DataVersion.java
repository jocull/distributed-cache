package com.github.jocull.raftcache.lib.data;

import java.util.Collections;
import java.util.Map;

/**
 * A value of a key at a point in the state machine's history.
 *
 * <p>Optionally carries an expiration instant (epoch millis). {@link #NO_EXPIRY} means the
 * entry never expires; a positive value means the entry is expired for any
 * {@code now >= expiresAtEpochMillis}.
 */
class DataVersion {
    /** No expiration (never expires). */
    static final long NO_EXPIRY = Long.MAX_VALUE;

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

    /**
     * Whether the entry has expired at (or before) {@code nowEpochMillis}.
     */
    public boolean isExpired(long nowEpochMillis) {
        return expiresAtEpochMillis <= nowEpochMillis;
    }
}
