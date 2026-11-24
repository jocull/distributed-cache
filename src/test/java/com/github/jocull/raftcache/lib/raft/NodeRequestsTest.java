package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeRequestsTest {

    @Test
    void testPendingRegistryGuards() throws Exception {
        final NodeRequests registry = new NodeRequests();

        // (6u) null request carrier — pre-fix: NPE at the pendingRequests.put key dereference
        assertThrows(IllegalArgumentException.class, () -> registry.getRequestFuture(null, StateResponse.class));

        // (6u) null response class — pre-fix: the call succeeds and the NPE (`isAssignableFrom(null)`)
        // is deferred into the caller's completed future — the wrong family at the wrong time
        final StateRequest request = new StateRequest();
        assertThrows(IllegalArgumentException.class, () -> registry.getRequestFuture(request, null));

        // (6u) null response at the completion intake — pre-fix: NPE
        assertThrows(IllegalArgumentException.class, () -> registry.completeRequest(null));

        // (6u) null requestId — the key axis: construction-bypassed carriers the Request/Response
        // constructors would never produce (a deserialized stream, a hostile override). Pre-fix both
        // sides are silently accepted: a null-keyed map entry collides with every other null-id
        // request, and a completion under that key can resolve the wrong pending future.
        assertThrows(IllegalArgumentException.class, () -> registry.getRequestFuture(new NullIdRequest(), StateResponse.class));
        assertThrows(IllegalArgumentException.class, () -> registry.completeRequest(new NullIdResponse(request)));

        // Discriminator — a legitimate round trip still completes with the exact response the caller awaited
        final StateRequest liveRequest = new StateRequest();
        final CompletableFuture<StateResponse> pending = registry.getRequestFuture(liveRequest, StateResponse.class);
        final StateResponse liveResponse = new StateResponse(liveRequest, "node-1", new NodeAddress("127.0.0.1:0"),
                NodeStates.FOLLOWER, 1, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(1, 1L),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(1, 1L));
        registry.completeRequest(liveResponse);
        assertSame(liveResponse, pending.get(1, TimeUnit.SECONDS));

        // Discriminator — a legitimate-but-unknown requestId is still a silent no-op (not an error, no completion)
        registry.completeRequest(new StateResponse(new StateRequest(), "node-2", new NodeAddress("127.0.0.1:1"),
                NodeStates.LEADER, 2, new com.github.jocull.raftcache.lib.raft.messages.TermIndex(2, 2L),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(2, 2L)));
    }

    /** A construction-bypassed request carrier: the requestId the constructor would never have produced. */
    private static final class NullIdRequest extends StateRequest {
        @Override
        public UUID getRequestId() {
            return null;
        }
    }

    /** A construction-bypassed response carrier: the requestId the constructor would never have produced. */
    private static final class NullIdResponse extends StateResponse {
        NullIdResponse(Request request) {
            super(request, "node-3", new NodeAddress("127.0.0.1:2"), NodeStates.FOLLOWER, 3,
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(3, 3L),
                    new com.github.jocull.raftcache.lib.raft.messages.TermIndex(3, 3L));
        }

        @Override
        public UUID getRequestId() {
            return null;
        }
    }
}
