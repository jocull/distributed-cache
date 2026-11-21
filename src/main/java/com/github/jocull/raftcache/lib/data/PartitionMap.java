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
        return partitionMap.get(getPartitionForKey(key))
                .getEntry(key)
                .map(dataVersion -> new KeyDataOutput(key, dataVersion.getData(), dataVersion.getVersion()));
    }

    public void setData(KeyDataInput input) {
        final long version = globalVersion.incrementAndGet();
        partitionMap.get(getPartitionForKey(input.getKey())).setEntry(input.getKey(), new DataVersion(input.getData(), version));
    }

    public boolean deleteData(String key) {
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

    public Map<Partition, Map<String, KeyDataOutput>> getSnapshot() {
        // TODO: Semaphore lock the world while doing this?
        //       Return an object that contains the global version at snapshot time?
        //       Make the return object a better interface?
        final Map<Partition, Map<String, KeyDataOutput>> snapshot = new HashMap<>(partitionMap.size());
        partitionMap.forEach((k, v) -> snapshot.put(k, v.getSnapshot()));
        return snapshot;
    }
}
