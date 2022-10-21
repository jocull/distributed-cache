package com.codefromjames.com.lib.event.events;

import java.util.Map;

public class SetDataEvent {
    private final String key;
    private final Map<String, byte[]> data;
    private final long version;

    public SetDataEvent(String key, Map<String, byte[]> data, long version) {
        this.key = key;
        this.data = data;
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
    public String toString() {
        return "SetDataEvent{" +
                "key='" + key + '\'' +
                ", data=" + data +
                ", version=" + version +
                '}';
    }
}
