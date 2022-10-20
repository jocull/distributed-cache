package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.event.events.NewDataEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class KeyData {
    private final KeyMap parent;
    private final ReentrantLock lock = new ReentrantLock();
    private final String key;
    private DataVersion dataVersion;

    public KeyData(String key, KeyMap parent) {
        this.key = key;
        this.parent = parent;
    }

    private static class DataVersion {
        private final Map<String, byte[]> data;
        private final long version;

        public DataVersion(Map<String, byte[]> data, long version) {
            this.data = data;
            this.version = version;
        }
    }

    public String getKey() {
        return key;
    }

    public Optional<KeyPayload> getData() {
        lock.lock();
        try {
            if (dataVersion != null) {
                return Optional.of(new KeyPayload(key, dataVersion.data));
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public void setData(KeyPayload payload, long version) {
        lock.lock();
        try {
            dataVersion = new DataVersion(payload.getData(), version);
            parent.getEventBus().publish(new NewDataEvent(
                    payload.getKey(),
                    new HashMap<>(payload.getData()),
                    version));
        } finally {
            lock.unlock();
        }
    }
}
