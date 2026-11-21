package com.github.jocull.raftcache.lib;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A test clock that starts at a fixed instant and can be advanced explicitly, so tests can
 * pin or move an entry's expiration deterministically instead of sleeping in wall-clock time.
 */
public class MutableClock extends Clock {
    private final ZoneId zone;
    private Instant instant;

    public MutableClock(Instant instant) {
        this.instant = instant;
        this.zone = ZoneOffset.UTC;
    }

    private MutableClock(Instant instant, ZoneId zone) {
        this.instant = instant;
        this.zone = zone;
    }

    public Instant current() {
        return instant;
    }

    /** Moves the clock to exactly {@code instant}. */
    public void set(Instant instant) {
        this.instant = instant;
    }

    /** Advances the clock by {@code millis} (must be non-negative). */
    public void advanceMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("Clock can only move forward. Provided: " + millis);
        }
        this.instant = this.instant.plusMillis(millis);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
