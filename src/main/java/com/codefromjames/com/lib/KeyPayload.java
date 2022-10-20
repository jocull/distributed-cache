package com.codefromjames.com.lib;

import java.util.Map;
import java.util.Objects;

public class KeyPayload {
    private final String key;
    private final Map<String, byte[]> data;

    public KeyPayload(String key, Map<String, byte[]> data) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(data);

        this.key = key;
        this.data = data;
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
        KeyPayload payload = (KeyPayload) o;
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
