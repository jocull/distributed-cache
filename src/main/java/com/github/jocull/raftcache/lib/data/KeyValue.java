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
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
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
     *
     * @throws IllegalArgumentException if {@code data} is null
     */
    public synchronized void updateExpiration(DataVersion data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        dataVersion = data;
    }

    /**
     * Replaces the stored version without publishing <em>any</em> event -- used when
     * adopting a {@link Snapshot}: that reinstalls already-committed state rather than
     * performing a new state-machine write, so neither a {@code SetDataEvent} nor a
     * {@code DeleteDataEvent} is expected. Unlike {@link #setData(DataVersion)} the data
     * itself changes; unlike {@link #updateExpiration(DataVersion)} no event is published.
     *
     * @throws IllegalArgumentException if {@code data} is null
     */
    public synchronized void restore(DataVersion data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        dataVersion = data;
    }
}
