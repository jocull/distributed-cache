package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.event.EventBus;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionMap {
    private final EventBus eventBus;
    private final AtomicLong globalVersion = new AtomicLong(0L);
    private final Map<Partition, KeyMap> partitionMap = new HashMap<>();
    private final Clock clock;

    public PartitionMap(EventBus eventBus, int size) {
        this(eventBus, size, Clock.systemUTC());
    }

    /**
     * Same as above but with an injectable {@link Clock} -- tests pin or advance an
     * expiring key's clock to exercise TTL expiry deterministically.
     */
    public PartitionMap(EventBus eventBus, int size, Clock clock) {
        Objects.requireNonNull(eventBus);
        if (size < 1) {
            throw new IllegalArgumentException("Size must be > 0. Provided: " + size);
        }
        Objects.requireNonNull(clock);

        this.eventBus = eventBus;
        this.clock = clock;
        for (int i = 0; i < size; i++) {
            final Partition partition = new Partition(i);
            partitionMap.put(partition, new KeyMap(this, partition));
        }
    }

    /**
     * The clock used to resolve entry expirations. Expiration is evaluated
     * independently on each node against this local clock; sub-millisecond drift
     * between nodes is acceptable for a cache.
     */
    public Clock getClock() {
        return clock;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public int getPartitionCount() {
        return partitionMap.size();
    }

    public Partition getPartitionForKey(String key) {
        return new Partition(Math.abs(key.hashCode() % partitionMap.size()));
    }

    public Optional<KeyDataOutput> getData(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return partitionMap.get(getPartitionForKey(key))
                .getEntry(key)
                .map(dataVersion -> new KeyDataOutput(key, dataVersion.getData(), dataVersion.getVersion()));
    }

    public void setData(KeyDataInput input) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (input.getKey() == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (input.getData() == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        final long version = globalVersion.incrementAndGet();
        partitionMap.get(getPartitionForKey(input.getKey())).setEntry(input.getKey(), new DataVersion(input.getData(), version));
    }

    public boolean deleteData(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        final long version = globalVersion.incrementAndGet();
        return partitionMap.get(getPartitionForKey(key)).deleteEntry(key, version);
    }

    /**
     * Applies an EXPIRE to {@code input.getKey()} -- its expiration becomes "now + ttl"
     * where "now" is this map's {@link #getClock()}.
     *
     * @return whether the entry existed and was live (a key that is absent or already
     *         expired is not resurrected -- the replicated no-op case)
     */
    public boolean expireData(KeyDataExpireInput input) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (input.getKey() == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (input.getTtl() == null) {
            throw new IllegalArgumentException("ttl must not be null");
        }
        final long version = globalVersion.incrementAndGet();
        final long now = clock.millis();
        return partitionMap.get(getPartitionForKey(input.getKey()))
                .expireEntry(input.getKey(), version, now, input.getTtl().toMillis());
    }

    /**
     * Removes every entry whose expiration has passed, publishing a
     * {@link com.github.jocull.raftcache.lib.event.events.DeleteDataEvent} per removal.
     *
     * <p>Entries are served as absent as soon as they expire ({@link #getData}); this is
     * the background cleanup that actually drops them from memory.
     *
     * @return the number of entries removed
     */
    public int sweepExpired() {
        final long now = clock.millis();
        int removed = 0;
        for (final KeyMap keyMap : partitionMap.values()) {
            removed += keyMap.sweepExpired(now);
        }
        return removed;
    }

    /**
     * Advances and returns the next global version -- used by {@link KeyMap}'s sweep to
     * stamp each removal with its own monotonically increasing version.
     */
    public long nextVersion() {
        return globalVersion.incrementAndGet();
    }

    /**
     * Captures the state machine's full data state now (SNAPSHOTS.md stage 1): for every
     * key, its data, version counter and absolute expiration. Entries already expired at
     * this map's {@link #getClock()} are no longer part of the state and are not captured.
     *
     * <p>This is the state half of a {@link Snapshot}; the boundary (term, index) belongs
     * to the state machine ({@link StateMachineBridge#getSnapshot()}) and the raft log,
     * not to the data layer.
     *
     * <p>The returned map is a detached copy: {@link DataVersion}s are immutable, and the
     * map itself is a fresh copy, so later mutations do not disturb a captured state.
     */
    public Map<String, DataVersion> capture() {
        final Map<String, DataVersion> state = new HashMap<>();
        for (final KeyMap keyMap : partitionMap.values()) {
            state.putAll(keyMap.capture());
        }
        return state;
    }

    /**
     * Replaces the full data state with {@code state} (see {@link #capture()}): keys
     * absent from it are dropped, and each captured key is reinstalled with its captured
     * data, version and absolute expiration.
     *
     * <p>Installs via the silent state-machine path -- no state-machine events are
     * published, because adopting a snapshot reinstalls already-committed state rather
     * than performing new writes.
     *
     * <p>The monotonic version counter is preserved: when the captured state carries a
     * higher version than the local counter (a snapshot of a state machine that had
     * advanced further), the counter is fast-forwarded so subsequent local writes are
     * still strictly newer.
     */
    public synchronized void restore(Map<String, DataVersion> state) {
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        // Validate every entry before the state is touched at all: a null key or
        // value -- a shape a captured state would never carry, but a HashMap would --
        // refuses the whole restore with zero state change (no partition mutation,
        // no version fast-forward).
        for (final Map.Entry<String, DataVersion> entry : state.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("key must not be null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("state value must not be null");
            }
        }
        long maxVersion = 0L;
        final Map<Partition, Map<String, DataVersion>> byPartition = new HashMap<>(partitionMap.size());
        for (final Map.Entry<String, DataVersion> entry : state.entrySet()) {
            byPartition.computeIfAbsent(getPartitionForKey(entry.getKey()), p -> new HashMap<>())
                    .put(entry.getKey(), entry.getValue());
            if (entry.getValue().getVersion() > maxVersion) {
                maxVersion = entry.getValue().getVersion();
            }
        }
        for (final Map.Entry<Partition, KeyMap> partitionEntry : partitionMap.entrySet()) {
            partitionEntry.getValue().restore(byPartition.getOrDefault(partitionEntry.getKey(), Map.of()));
        }
        if (maxVersion > globalVersion.get()) {
            globalVersion.set(maxVersion);
        }
    }
}
