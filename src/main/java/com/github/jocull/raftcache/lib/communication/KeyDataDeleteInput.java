package com.github.jocull.raftcache.lib.communication;

import java.util.Objects;

/**
 * A replicated "delete key" command for the state machine.
 *
 * <p>{@link KeyDataInput} models a PUT; this entry models a DELETE. It carries only the key --
 * the state machine needs nothing else to remove an entry, and there is no data to capture.
 *
 * <p>Deleting a key that is absent is a no-op in the state machine
 * ({@link com.github.jocull.raftcache.lib.data.PartitionMap#deleteData(String)} returns
 * {@code false}) -- so replaying the same entry, or applying it on several nodes that share a
 * bus, is idempotent and safe under Raft.
 */
public class KeyDataDeleteInput {
    private final String key;

    public KeyDataDeleteInput(String key) {
        Objects.requireNonNull(key);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyDataDeleteInput payload = (KeyDataDeleteInput) o;
        return key.equals(payload.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "KeyDataDeleteInput{" +
                "key='" + key + '\'' +
                '}';
    }
}
