package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.event.EventBus;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyMap {
    private final PartitionMap parent;
    private final Map<String, KeyData> keyMap = new HashMap<>();

    public KeyMap(PartitionMap parent) {
        this.parent = parent;
    }

    EventBus getEventBus() {
        return parent.getEventBus();
    }

    public Optional<KeyPayload> getData(String key) {
        final KeyData keyData;
        synchronized (keyMap) {
            keyData = keyMap.get(key);
        }
        if (keyData == null) {
            return Optional.empty();
        }
        return keyData.getData();
    }

    public void setData(KeyPayload payload, long version) {
        final KeyData keyData;
        synchronized (keyMap) {
            keyData = keyMap.compute(payload.getKey(), (k, v) -> {
                if (v == null) {
                    return new KeyData(payload.getKey(), this);
                }
                return v;
            });
        }
        keyData.setData(payload, version);
    }

    public void getSnapshot() {
        // keyMap.forEach((k, v) -> new ???);
        throw new UnsupportedOperationException();
    }
}
