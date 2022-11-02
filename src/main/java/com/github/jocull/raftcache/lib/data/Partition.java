package com.github.jocull.raftcache.lib.data;

import java.util.Objects;

public class Partition {
    private final int partition;

    public Partition(int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition partition1 = (Partition) o;
        return partition == partition1.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partition=" + partition +
                '}';
    }
}
