package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.communication.KeyDataOutput;
import com.codefromjames.com.lib.communication.KeyDataInput;
import com.codefromjames.com.lib.event.EventBus;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionMap {
    private final EventBus eventBus;
    private final AtomicLong globalVersion = new AtomicLong(0L);
    private final Map<Partition, KeyMap> partitionMap = new HashMap<>();

    public PartitionMap(EventBus eventBus, int size) {
        Objects.requireNonNull(eventBus);
        if (size < 1) {
            throw new IllegalArgumentException("Size must be > 0. Provided: " + size);
        }

        this.eventBus = eventBus;
        for (int i = 0; i < size; i++) {
            final Partition partition = new Partition(i);
            partitionMap.put(partition, new KeyMap(this, partition));
        }
    }

    EventBus getEventBus() {
        return eventBus;
    }

    public int getPartitionCount() {
        return partitionMap.size();
    }

    public Partition partitionForKey(String key) {
        return new Partition(Math.abs(key.hashCode() % partitionMap.size()));
    }

    public Optional<KeyDataOutput> getData(String key) {
        return partitionMap.get(partitionForKey(key))
                .getData(key)
                .map(dataVersion -> new KeyDataOutput(key, dataVersion.getData(), dataVersion.getVersion()));
    }

    public void setData(KeyDataInput input) {
        final long version = globalVersion.incrementAndGet();
        partitionMap.get(partitionForKey(input.getKey())).setData(input.getKey(), new DataVersion(input.getData(), version));
    }

    private void getReplicationSnapshot() {
        throw new UnsupportedOperationException();
    }
}
