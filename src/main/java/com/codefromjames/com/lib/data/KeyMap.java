package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.event.EventBus;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class KeyMap {
    private final PartitionMap parent;
    private final Partition partition;

    private final Map<String, KeyValue> keyMap = new HashMap<>();

    public KeyMap(PartitionMap parent, Partition partition) {
        this.parent = parent;
        this.partition = partition;
    }

    EventBus getEventBus() {
        return parent.getEventBus();
    }

    public Optional<DataVersion> getData(String key) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.get(key);
        }
        if (keyValue == null) {
            return Optional.empty();
        }
        return keyValue.getData();
    }

    public void setData(String key, DataVersion dataVersion) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.compute(key, (k, v) -> {
                if (v == null) {
                    return new KeyValue(key, this);
                }
                return v;
            });
        }
        keyValue.setData(dataVersion);
    }

    public void getSnapshot() {
        // keyMap.forEach((k, v) -> new ???);
        throw new UnsupportedOperationException();
    }
}