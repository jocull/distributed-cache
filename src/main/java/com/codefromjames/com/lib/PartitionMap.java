package com.codefromjames.com.lib;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PartitionMap {
    private final Map<Partition, KeyMap> partitionMap = new HashMap<>();

    public PartitionMap(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("Size must be > 0. Provided: " + size);
        }

        for (int i = 0; i < size; i++) {
            partitionMap.put(new Partition(i), new KeyMap());
        }
    }

    public int getPartitionCount() {
        return partitionMap.size();
    }

    public Partition partitionForKey(String key) {
        return new Partition(Math.abs(key.hashCode() % partitionMap.size()));
    }

    public Optional<KeyPayload> getData(String key) {
        return partitionMap.get(partitionForKey(key)).getData(key);
    }

    public void setData(KeyPayload payload) {
        partitionMap.get(partitionForKey(payload.getKey())).setData(payload);
    }
}
