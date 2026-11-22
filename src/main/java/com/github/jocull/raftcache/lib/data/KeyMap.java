package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.events.DeleteDataEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class KeyMap {
    private final PartitionMap parent;
    private final Partition partition;

    private final Map<String, KeyValue> keyMap = new HashMap<>();

    public KeyMap(PartitionMap parent, Partition partition) {
        this.parent = parent;
        this.partition = partition;
    }

    EventBus getEventBus() {
        return parent.getEventBus();
    }

    public Optional<DataVersion> getEntry(String key) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.get(key);
        }
        if (keyValue == null) {
            return Optional.empty();
        }
        final Optional<DataVersion> data = keyValue.getData();
        // Checked read: an expired entry is served as absent (the entry stays in the map
        // until a sweep removes it -- the same lazy-expiration shape as the classic
        // delete-on-read, with removal done by sweepExpired)
        if (data.isEmpty() || data.get().isExpired(parent.getClock().millis())) {
            return Optional.empty();
        }
        return data;
    }

    public void setEntry(String key, DataVersion dataVersion) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.compute(key, (k, v) -> {
                if (v == null) {
                    return new KeyValue(key, this);
                }
                return v;
            });
        }
        keyValue.setData(dataVersion);
    }

    public boolean deleteEntry(String key, long version) {
        final KeyValue removed;
        synchronized (keyMap) {
            removed = keyMap.remove(key);
        }
        if (removed != null) {
            getEventBus().publish(new DeleteDataEvent(key, version));
            return true;
        }
        return false;
    }

    /**
     * Sets the expiration of {@code key}'s entry to {@code nowEpochMillis + ttlMillis},
     * recording the new version.
     *
     * <p>Returns {@code false} when the key is absent or already expired at {@code
     * nowEpochMillis} -- an expired entry is never resurrected by a late EXPIRE. No
     * {@code SetDataEvent} is published: the value is unchanged (see
     * {@link KeyValue#updateExpiration(DataVersion)}).
     */
    public boolean expireEntry(String key, long version, long nowEpochMillis, long ttlMillis) {
        final KeyValue keyValue;
        synchronized (keyMap) {
            keyValue = keyMap.get(key);
        }
        if (keyValue == null) {
            return false;
        }
        final Optional<DataVersion> current = keyValue.getData();
        if (current.isEmpty() || current.get().isExpired(nowEpochMillis)) {
            return false;
        }
        keyValue.updateExpiration(new DataVersion(current.get().getData(), version, nowEpochMillis + ttlMillis));
        return true;
    }

    /**
     * Removes every entry of this partition that is expired at {@code nowEpochMillis}.
     *
     * <p>The check-and-remove happens atomically per partition (the iteration holds the map
     * lock), so an entry that is re-written -- fresh, unexpired -- between the sweep's start
     * and its check is never removed. Each removal consumes the next global version and
     * publishes a {@link DeleteDataEvent}, the same contract as the replicated delete path.
     *
     * @return the number of entries removed
     */
    public int sweepExpired(long nowEpochMillis) {
        final List<String> expired = new ArrayList<>();
        synchronized (keyMap) {
            final Iterator<Map.Entry<String, KeyValue>> iterator = keyMap.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<String, KeyValue> entry = iterator.next();
                final Optional<DataVersion> data = entry.getValue().getData();
                if (data.isPresent() && data.get().isExpired(nowEpochMillis)) {
                    expired.add(entry.getKey());
                    iterator.remove();
                }
            }
        }
        for (final String key : expired) {
            getEventBus().publish(new DeleteDataEvent(key, parent.nextVersion()));
        }
        return expired.size();
    }

    /**
     * A point-in-time capture of this partition's state (SNAPSHOTS.md stage 1): for each
     * key, the full {@link DataVersion} -- data, version counter and absolute expiration.
     * Entries already expired at this map's clock are no longer part of the state and are
     * not captured.
     *
     * <p>In contrast to the client read view ({@link PartitionMap#getData(String)} serves
     * a {@link KeyDataOutput} which drops the expiration), the capture carries the
     * expiration -- the gap that made the old {@code getSnapshot()} unusable for
     * snapshots.
     */
    public Map<String, DataVersion> capture() {
        final long now = parent.getClock().millis();
        final Map<String, DataVersion> state = new HashMap<>(keyMap.size());
        synchronized (keyMap) {
            for (final Map.Entry<String, KeyValue> entry : keyMap.entrySet()) {
                final Optional<DataVersion> dataVersion = entry.getValue().getData();
                if (dataVersion.isPresent() && !dataVersion.get().isExpired(now)) {
                    state.put(entry.getKey(), dataVersion.get());
                }
            }
        }
        return state;
    }

    /**
     * Replaces this partition's entries with exactly {@code state} (see {@link #capture()}).
     *
     * <p>Installs via the silent state-machine path ({@link KeyValue#restore(DataVersion)}):
     * adopting a snapshot reinstalls already-committed state, so no
     * {@code SetDataEvent}/{@code DeleteDataEvent} is published.
     */
    public synchronized void restore(Map<String, DataVersion> state) {
        synchronized (keyMap) {
            keyMap.clear();
            for (final Map.Entry<String, DataVersion> entry : state.entrySet()) {
                final KeyValue keyValue = new KeyValue(entry.getKey(), this);
                keyValue.restore(entry.getValue());
                keyMap.put(entry.getKey(), keyValue);
            }
        }
    }
}
