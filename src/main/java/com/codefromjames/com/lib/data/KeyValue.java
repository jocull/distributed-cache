package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.event.events.NewDataEvent;

import java.util.Optional;

class KeyValue {
    private final KeyMap parent;

    private final String key;
    private DataVersion dataVersion;

    public KeyValue(String key, KeyMap parent) {
        this.key = key;
        this.parent = parent;
    }

    public String getKey() {
        return key;
    }

    public Optional<DataVersion> getData() {
        synchronized (this) {
            return Optional.ofNullable(dataVersion);
        }
    }

    public void setData(DataVersion data) {
        synchronized (this) {
            dataVersion = data;
            parent.getEventBus().publish(new NewDataEvent(
                    key,
                    data.getData(),
                    data.getVersion()));
        }
    }
}
