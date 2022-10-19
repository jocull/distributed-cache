package com.codefromjames.com.lib;

import java.util.Arrays;
import java.util.Objects;

public class KeyPayload {
    private final String key;
    private final byte[] data;

    public KeyPayload(String key, byte[] data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyPayload that = (KeyPayload) o;
        return Objects.equals(key, that.key) && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "KeyPayload{" +
                "key='" + key + '\'' +
                ", data=" + (data == null ? "null" : data.length) + " bytes" +
                '}';
    }
}
