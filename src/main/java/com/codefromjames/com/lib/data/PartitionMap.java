package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.communication.KeyDataOutput;
import com.codefromjames.com.lib.communication.KeyDataInput;
import com.codefromjames.com.lib.event.EventBus;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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

    public Map<Partition, Map<String, KeyDataOutput>> getSnapshot() {
        // TODO: Semaphore lock the world while doing this?
        //       Return an object that contains the global version at snapshot time?
        //       Make the return object a better interface?
        final Map<Partition, Map<String, KeyDataOutput>> snapshot = new HashMap<>(partitionMap.size());
        partitionMap.forEach((k, v) -> snapshot.put(k, v.getSnapshot()));
        return snapshot;
    }
}
