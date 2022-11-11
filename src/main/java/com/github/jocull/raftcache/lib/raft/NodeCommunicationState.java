package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.topology.NodeAddress;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public interface NodeCommunicationState {
    String getRemoteNodeId();

    void setRemoteNodeId(String remoteNodeId);

    NodeAddress getRemoteNodeAddress();

    TermIndex getTermIndex();

    void setTermIndex(TermIndex termIndex);

    Instant getLastEntriesSent();

    void setLastEntriesSent(Instant lastEntriesSent);

    Instant getLastEntriesAcknowledged();

    void setLastEntriesAcknowledged(Instant lastEntriesAcknowledged);

    default boolean shouldSendNextEntries(int heartbeatMillis) {
        // must have received after last send
        if (getLastEntriesAcknowledged().isAfter(getLastEntriesSent())) {
            return true;
        }

        // ...OR last send must be < now - heartbeat millis
        final Instant heartbeatLimit = getLastEntriesSent().plus(heartbeatMillis, ChronoUnit.MILLIS);
        if (Instant.now().isAfter(heartbeatLimit)) {
            return true;
        }

        return false;
    }
}
