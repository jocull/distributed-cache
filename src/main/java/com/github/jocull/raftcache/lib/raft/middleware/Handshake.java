package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

import java.io.Serializable;
import java.util.Objects;

/**
 * The first frame a {@link SocketChannelMiddleware} client writes on a new connection:
 * who is dialing (client) and which logical node the dial is addressed to (target). The
 * server validates the target against its node registry, installs its receiver, and echoes
 * an {@link Acknowledgement} before the client unblocks (see {@code NETWORKING.md} 2.2).
 */
public class Handshake implements Serializable {
    private static final long serialVersionUID = 1L;

    private final NodeAddress client;
    private final NodeAddress target;

    public Handshake(NodeAddress client, NodeAddress target) {
        this.client = Objects.requireNonNull(client, "client");
        this.target = Objects.requireNonNull(target, "target");
    }

    public NodeAddress getClient() {
        return client;
    }

    public NodeAddress getTarget() {
        return target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Handshake handshake = (Handshake) o;
        return client.equals(handshake.client) && target.equals(handshake.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client, target);
    }

    @Override
    public String toString() {
        return "Handshake{" +
                "client=" + client +
                ", target=" + target +
                '}';
    }
}
