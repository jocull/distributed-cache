package com.codefromjames.com.lib.data;

import com.codefromjames.com.lib.event.events.SetDataEvent;

import java.util.Optional;

class KeyValue {
    private final KeyMap parent;

    private final String key;
    private DataVersion dataVersion;

    public KeyValue(String key, KeyMap parent) {
        this.key = key;
        this.parent = parent;
    }

    public synchronized Optional<DataVersion> getData() {
        return Optional.ofNullable(dataVersion);
    }

    public void setData(DataVersion data) {
        final SetDataEvent event;
        synchronized (this) {
            dataVersion = data;
            event = new SetDataEvent(key, data.getData(), data.getVersion());
        }
        parent.getEventBus().publish(event);
    }
}
