package com.codefromjames.com.lib;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class KeyData {
    private final ReentrantLock lock = new ReentrantLock();
    private final Deque<DataVersion> dataVersions = new ArrayDeque<>();

    private static class DataVersion {
        private final byte[] data;
        private final long version;

        public DataVersion(byte[] data, long version) {
            this.data = data;
            this.version = version;
        }
    }

    public Optional<KeyPayload> getData(String key) {
        lock.lock();
        try {
            final DataVersion dataVersion = dataVersions.peekLast();
            if (dataVersion != null) {
                return Optional.of(new KeyPayload(key, dataVersion.data));
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public void setData(KeyPayload payload) {
        lock.lock();
        try {
            // TODO: Implement versioning, remove old versions
            final DataVersion dataVersion = new DataVersion(payload.getData(), 0);
            dataVersions.clear();
            dataVersions.push(dataVersion);
        } finally {
            lock.unlock();
        }
    }
}
