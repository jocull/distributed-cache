package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.communication.KeyDataOutput;
import com.codefromjames.com.lib.event.EventBus;
import com.codefromjames.com.lib.event.events.DeleteDataEvent;

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

    public Optional<DataVersion> getEntry(String key) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.get(key);
        }
        if (keyValue == null) {
            return Optional.empty();
        }
        return keyValue.getData();
    }

    public void setEntry(String key, DataVersion dataVersion) {
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

    public boolean deleteEntry(String key, long version) {
        final KeyValue removed;
        synchronized (keyMap) {
            removed = keyMap.remove(key);
        }
        if (removed != null) {
            getEventBus().publish(new DeleteDataEvent(key, version));
            return true;
        }
        return false;
    }

    public Map<String, KeyDataOutput> getSnapshot() {
        synchronized (keyMap) {
            Map<String, KeyDataOutput> snapshot = new HashMap<>(keyMap.size());
            keyMap.forEach((k, v) -> {
                if (v.getData().isPresent()) {
                    snapshot.put(k, new KeyDataOutput(k, v.getData().get().getData(), v.getData().get().getVersion()));
                }
            });
            return snapshot;
        }
    }
}
