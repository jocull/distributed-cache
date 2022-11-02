package com.github.jocull.raftcache.lib.topology;

import java.util.Objects;

public class NodeAddress {
    private final String address;

    public NodeAddress(String address) {
        Objects.requireNonNull(address);

        this.address = address;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeAddress that = (NodeAddress) o;
        return address.equals(that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }

    @Override
    public String toString() {
        return "NodeAddress{" +
                "address='" + address + '\'' +
                '}';
    }
}
