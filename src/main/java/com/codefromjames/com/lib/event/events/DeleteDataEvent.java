package com.codefromjames.com.lib.event.events;

public class DeleteDataEvent {
    private final String key;
    private final long version;

    public DeleteDataEvent(String key, long version) {
        this.key = key;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "DeleteDataEvent{" +
                "key='" + key + '\'' +
                ", version=" + version +
                '}';
    }
}
