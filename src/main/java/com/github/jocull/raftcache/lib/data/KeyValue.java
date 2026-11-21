package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.event.events.SetDataEvent;

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

    /**
     * Replaces the stored version without publishing a {@link SetDataEvent} -- the value is
     * unchanged, only its expiration moved (a replicated EXPIRE). Value-change events stay
     * reserved for {@link #setData(DataVersion)}.
     */
    public synchronized void updateExpiration(DataVersion data) {
        dataVersion = data;
    }
}
