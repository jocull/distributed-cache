package com.github.jocull.raftcache.lib.communication;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * A replicated "expire key" command for the state machine.
 *
 * <p>{@link KeyDataInput} models a SET and {@link KeyDataDeleteInput} a DELETE; this entry
 * models an EXPIRE: it sets the expiration of an existing entry to "now + {@code ttl}".
 *
 * <p>EXPIRE on a key that is absent (or already expired) is a no-op in the state machine --
 * it never resurrects a key, so replaying the entry, or applying it on several nodes that
 * share a bus, is idempotent and safe under Raft.
 *
 * <p>{@code ttl} is the time-to-live relative to the moment the entry is <em>applied</em>;
 * it is deliberately not resolved when the entry is submitted (the leader and the followers
 * apply it at different instants).
 */
public class KeyDataExpireInput implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;
    private final Duration ttl;

    public KeyDataExpireInput(String key, Duration ttl) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(ttl);
        // Note: Duration has isNegative()/isZero() but not isPositive() (that is a Period
        // method), and this toolchain's --release 17 profile is missing it anyway
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive. Provided: " + ttl);
        }

        this.key = key;
        this.ttl = ttl;
    }

    public String getKey() {
        return key;
    }

    public Duration getTtl() {
        return ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyDataExpireInput that = (KeyDataExpireInput) o;
        return key.equals(that.key) && ttl.equals(that.ttl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, ttl);
    }

    @Override
    public String toString() {
        return "KeyDataExpireInput{" +
                "key='" + key + '\'' +
                ", ttl=" + ttl +
                '}';
    }
}
