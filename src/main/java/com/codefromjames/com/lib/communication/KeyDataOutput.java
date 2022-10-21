package com.codefromjames.com.lib.communication;

import java.util.Map;
import java.util.Objects;

public class KeyDataOutput {
    private final String key;
    private final Map<String, byte[]> data;
    private final long version;

    public KeyDataOutput(String key, Map<String, byte[]> data, long version) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(data);

        this.key = key;
        this.data = Map.copyOf(data);
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyDataOutput that = (KeyDataOutput) o;
        return version == that.version && key.equals(that.key) && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, data, version);
    }

    @Override
    public String toString() {
        return "KeyDataOutput{" +
                "key='" + key + '\'' +
                ", data=" + data +
                ", version=" + version +
                '}';
    }
}
