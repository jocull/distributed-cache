package com.github.jocull.raftcache.lib.communication;

import java.util.Map;
import java.util.Objects;

public class KeyDataInput {
    private final String key;
    private final Map<String, byte[]> data;

    public KeyDataInput(String key, Map<String, byte[]> data) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(data);

        this.key = key;
        this.data = Map.copyOf(data); // Immutable!
    }

    public String getKey() {
        return key;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyDataInput payload = (KeyDataInput) o;
        return key.equals(payload.key) && data.equals(payload.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, data);
    }

    @Override
    public String toString() {
        return "KeyPayload{" +
                "key='" + key + '\'' +
                ", data=" + data +
                '}';
    }
}
