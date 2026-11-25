package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.MutableClock;
import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeConnectionOutbound;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.CacheDeleteRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheExpireRequest;
import com.github.jocull.raftcache.lib.raft.messages.CachePutRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadResponse;
import com.github.jocull.raftcache.lib.raft.messages.CacheStatus;
import com.github.jocull.raftcache.lib.raft.messages.CacheWriteResponse;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.Response;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client B §3.4 (2026-11-25, B2) — the remote cache verbs driven by a RAW WIRE CLIENT over
 * real sockets instead of the in-JVM facade. The client is {@link ClientConnection}: a
 * {@link NodeConnectionOutbound} over a fresh {@link SocketChannelMiddleware}'s
 * {@link ChannelSide} that completes the handshake and then speaks cache verbs only. It never
 * performs Introduction (B §4.1 pre-introduction dispatch — the first frame is a client verb)
 * and it holds no {@link RaftNode} reference (B doc §5, ":243" acceptance: "real client over
 * sockets, no RaftNode ref").
 *
 * <p>Covered here (of B doc §5): write durability — an OK reply arrives only AFTER commit and
 * leader-local apply, so read-your-own-write holds the moment the future completes; cross-node
 * LWW convergence of wire writes; local-serving reads on any node (B §4.3); and the
 * no-leader-typed reply — a write to a follower is a clean NOT_LEADER, not a dropped or hung
 * request. Discovery, failover re-discovery and the client-owned timeout policy (B §4.6) land
 * with B3 and B4 — deliberately not exercised here.
 *
 * <p>Correlation is a local {@code requestId → future} map, shaped like the
 * {@code NodeRequests} book-keeping between nodes, minus its 5-second default cancel: B2 keeps
 * the request-level timeout on the (unbuilt) B3 client seam.
 */
class ClientBWireServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBWireServiceTest.class);
    private static final int NODES = 3;
    private static final int WAIT_SECONDS = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    // ---------------------------------------------------------------------------
    // Client-served B2 acceptance
    // ---------------------------------------------------------------------------

    @Test
    void c1ClientPutCommitsAppliesAndConverges() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("c1: no leader before the write", WAIT_SECONDS, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            final byte[] value = "wired-put".getBytes(StandardCharsets.UTF_8);

            try (ClientConnection client = openClient(kit, leader)) {
                final CacheWriteResponse response = client
                        .sendCachePut(new CachePutRequest(new KeyDataInput("c1-key", Map.of("value", value))))
                        .get(WAIT_SECONDS, UNIT);
                assertEquals(CacheStatus.OK, response.getStatus(), "c1: the leader must ack the wire PUT");

                // Read-your-own-write: the OK reply means commit + leader-local apply already
                // happened, so the leader's own local view holds the entry NOW (B doc §5:
                // "commit + apply before the future completes").
                final KeyDataOutput leaderView = kit.localViewOf(leaderIdx, "c1-key");
                assertArrayEquals(value, leaderView.getData().get("value"), "c1: leader-local value");

                // The ack proves the commit; now wait for every node's apply and check the
                // LWW-converged value on each local state machine (B doc §5).
                final long index = kit.cache(leaderIdx).getLastAppliedIndex();
                kit.awaitAppliedAll(index, "c1: wire PUT not applied on every node", WAIT_SECONDS, UNIT);
                for (int i = 0; i < NODES; i++) {
                    assertArrayEquals(value, kit.localViewOf(i, "c1-key").getData().get("value"),
                            "c1: node " + i + " value");
                }
            }
        }
    }

    @Test
    void c2ClientDeleteRemovesItEverywhereInTheCluster() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("c2: no leader before the writes", WAIT_SECONDS, UNIT);
            final int leaderIdx = kit.indexOf(leader);

            // Seed through the client too: the whole exchange is wire-only (client B, no facade).
            try (ClientConnection client = openClient(kit, leader)) {
                final byte[] value = "c2-value".getBytes(StandardCharsets.UTF_8);
                final CacheWriteResponse put = client
                        .sendCachePut(new CachePutRequest(new KeyDataInput("c2-key", Map.of("value", value))))
                        .get(WAIT_SECONDS, UNIT);
                assertEquals(CacheStatus.OK, put.getStatus(), "c2: the seed PUT must ack OK");
                assertArrayEquals(value, kit.localViewOf(leaderIdx, "c2-key").getData().get("value"),
                        "c2: seed visible on the leader");

                final CacheWriteResponse delete = client
                        .sendCacheDelete(new CacheDeleteRequest(new KeyDataDeleteInput("c2-key")))
                        .get(WAIT_SECONDS, UNIT);
                assertEquals(CacheStatus.OK, delete.getStatus(), "c2: the wire DELETE must ack OK");
            }

            final long index = kit.cache(leaderIdx).getLastAppliedIndex();
            kit.awaitAppliedAll(index, "c2: wire DELETE not applied on every node", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "c2-key"), "c2: node " + i + " must see the delete");
            }
        }
    }

    @Test
    void c3ClientExpireExpiresItClusterWide() throws Exception {
        // One cluster clock: expiration is resolved at read time against it, so advancing it
        // pins the deadline deterministically (no wall-clock sleeps) — the IT-4 discipline.
        final MutableClock clusterClock = new MutableClock(Instant.EPOCH);
        try (ClusterKit kit = new ClusterKit(clusterClock)) {
            final RaftNode leader = kit.awaitLeader("c3: no leader before the writes", WAIT_SECONDS, UNIT);
            final int leaderIdx = kit.indexOf(leader);

            try (ClientConnection client = openClient(kit, leader)) {
                final byte[] value = "c3-value".getBytes(StandardCharsets.UTF_8);
                final CacheWriteResponse put = client
                        .sendCachePut(new CachePutRequest(new KeyDataInput("ttl-key", Map.of("value", value))))
                        .get(WAIT_SECONDS, UNIT);
                assertEquals(CacheStatus.OK, put.getStatus(), "c3: the seed PUT must ack OK");
                final CacheWriteResponse expire = client
                        .sendCacheExpire(new CacheExpireRequest(new KeyDataExpireInput("ttl-key", Duration.ofSeconds(2L))))
                        .get(WAIT_SECONDS, UNIT);
                assertEquals(CacheStatus.OK, expire.getStatus(), "c3: the wire EXPIRE must ack OK");
            }

            final long index = kit.cache(leaderIdx).getLastAppliedIndex();
            kit.awaitAppliedAll(index, "c3: wire EXPIRE not applied on every node", WAIT_SECONDS, UNIT);
            for (int i = 0; i < NODES; i++) {
                assertTrue(kit.localHas(i, "ttl-key"), "c3: node " + i + " must still hold the key at its deadline");
            }

            clusterClock.advanceMillis(1500L); // still before the 2000ms deadline
            for (int i = 0; i < NODES; i++) {
                assertTrue(kit.localHas(i, "ttl-key"),
                        "c3: node " + i + " must still hold the key just before its deadline");
            }

            clusterClock.advanceMillis(600L); // 2100ms — past the deadline
            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "ttl-key"),
                        "c3: node " + i + " must read the key as expired on its checked-read path");
            }

            // And the wire read agrees with the facade's checked view — same local-serving
            // path (B §4.3), expired entries are absent reads, not errors.
            try (ClientConnection client = openClient(kit, leader)) {
                final CacheReadResponse response = client.sendCacheRead(new CacheReadRequest("ttl-key"))
                        .get(WAIT_SECONDS, UNIT);
                assertFalse(response.isFound(), "c3: the expired key must be a wire read miss");
                assertNull(response.getValue(), "c3: no value on a miss");
            }
        }
    }

    @Test
    void c4ClientReadServesTheLocalView() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("c4: no leader before the writes", WAIT_SECONDS, UNIT);
            final int leaderIdx = kit.indexOf(leader);
            final RaftNode follower = kit.followers().get(0);

            final byte[] a = "1".getBytes(StandardCharsets.UTF_8);
            final byte[] b = "2".getBytes(StandardCharsets.UTF_8);

            try (ClientConnection client = openClient(kit, leader)) {
                assertEquals(CacheStatus.OK, client
                        .sendCachePut(new CachePutRequest(new KeyDataInput("read-key", Map.of("a", a, "b", b))))
                        .get(WAIT_SECONDS, UNIT).getStatus(), "c4: the seed PUT must ack OK");

                // Hit over the wire: the reply carries the entry itself (key + data), not a
                // status flag alone.
                final CacheReadResponse hit = client.sendCacheRead(new CacheReadRequest("read-key"))
                        .get(WAIT_SECONDS, UNIT);
                assertTrue(hit.isFound(), "c4: the wire read must find the key it just wrote (read-your-own-write)");
                final KeyDataOutput value = hit.getValue();
                assertNotNull(value, "c4: a hit carries its value");
                assertEquals("read-key", value.getKey(), "c4: the value reports its key");
                assertEquals(Set.of("a", "b"), value.getData().keySet(), "c4: the value's data keys");
                assertArrayEquals(a, value.getData().get("a"), "c4: value of data key a");
                assertArrayEquals(b, value.getData().get("b"), "c4: value of data key b");

                // Miss over the wire: not-found, no value — a reply, not an error.
                final CacheReadResponse miss = client.sendCacheRead(new CacheReadRequest("never-written"))
                        .get(WAIT_SECONDS, UNIT);
                assertFalse(miss.isFound(), "c4: an absent key must miss");
                assertNull(miss.getValue(), "c4: no value on a miss");
            }

            // Local-serving (B §4.3): once the seed has converged, even a FOLLOWER answers
            // with its own replicated view.
            final long index = kit.cache(leaderIdx).getLastAppliedIndex();
            kit.awaitAppliedAll(index, "c4: the seed PUT not applied on every node", WAIT_SECONDS, UNIT);
            try (ClientConnection followerClient = openClient(kit, follower)) {
                final CacheReadResponse hit = followerClient.sendCacheRead(new CacheReadRequest("read-key"))
                        .get(WAIT_SECONDS, UNIT);
                assertTrue(hit.isFound(), "c4: the follower serves its replicated local view");
                assertArrayEquals(b, hit.getValue().getData().get("b"), "c4: follower value of data key b");
            }
        }
    }

    @Test
    void c5WriteToFollowerIsACleanNotLeader() throws Exception {
        try (ClusterKit kit = new ClusterKit()) {
            kit.awaitLeader("c5: no leader before the write", WAIT_SECONDS, UNIT);
            final RaftNode follower = kit.followers().get(0);

            try (ClientConnection client = openClient(kit, follower)) {
                final CacheWriteResponse response = client
                        .sendCachePut(new CachePutRequest(
                                new KeyDataInput("c5-key", Map.of("value", "x".getBytes(StandardCharsets.UTF_8)))))
                        .get(WAIT_SECONDS, UNIT);
                // B doc §5: the no-leader case is a TYPED RETRYABLE error — the client got a
                // normal NOT_LEADER reply on its own request id, not a dropped/hung request.
                assertEquals(CacheStatus.NOT_LEADER, response.getStatus(),
                        "c5: a follower must refuse the write with NOT_LEADER");
            }

            for (int i = 0; i < NODES; i++) {
                assertFalse(kit.localHas(i, "c5-key"), "c5: the refused write must land nowhere");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Test-side client
    // ---------------------------------------------------------------------------

    /**
     * Opens a raw wire channel to {@code target}: the client-side
     * {@link SocketChannelMiddleware} dials the target's bound address (server-side handshake
     * resolution needs exactly that string). {@code sourceAddress} only supplies the handshake's
     * client address — pick a NON-target node so the pair is not self-referential; the client
     * itself never introduces (B §4.1).
     */
    private static ClientConnection openClient(ClusterKit kit, RaftNode target) {
        final RaftNode sourceAddress = kit.node(kit.indexOf(target) == 0 ? 1 : 0);
        final SocketChannelMiddleware clientMw = new SocketChannelMiddleware();
        final ChannelMiddleware.ChannelSide side = clientMw.openChannel(sourceAddress, target.getNodeAddress());
        return new ClientConnection(side);
    }

    /**
     * A pure cache client for the B2 tests: the four request verbs send over
     * {@link ChannelSide#send} and correlate their reply frames by {@code requestId} (the
     * server echoes the request id into the reply). Everything else on
     * {@link NodeConnectionOutbound} fails loud: this stub is never a Raft peer, and a test
     * that reaches those methods is misusing the stub (or the wiring under test is broken).
     */
    private static final class ClientConnection implements NodeConnectionOutbound, AutoCloseable {
        private final ChannelMiddleware.ChannelSide channel;
        // CompletableFuture<Object>: the map must be completable with either reply type, so
        // futures are erased on the way in (unchecked) and typed on the way out (the caller
        // holds the real CompletableFuture<TResponse>).
        private final Map<UUID, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();

        private ClientConnection(ChannelMiddleware.ChannelSide channel) {
            this.channel = channel;
            channel.setReceiver(frame -> {
                if (frame instanceof CacheWriteResponse response) {
                    complete(response);
                } else if (frame instanceof CacheReadResponse response) {
                    complete(response);
                } else {
                    LOGGER.debug("Ignoring {} from {}", frame.getClass().getSimpleName(), channel.getAddress());
                }
            });
        }

        private void complete(Response response) {
            final CompletableFuture<Object> waiter = pending.remove(response.getRequestId());
            if (waiter == null) {
                LOGGER.debug("{} (request {}) answered a request the client no longer waits for",
                        response.getClass().getSimpleName(), response.getRequestId());
                return;
            }
            waiter.complete(response);
        }

        @SuppressWarnings("unchecked")
        private <TResponse> CompletableFuture<TResponse> sendVerb(Request request) {
            final CompletableFuture<TResponse> future = new CompletableFuture<>();
            pending.put(request.getRequestId(), (CompletableFuture<Object>) (CompletableFuture<?>) future);
            channel.send(request);
            return future;
        }

        @Override
        public void close() {
            channel.close();
        }

        // ---- Client B verbs (the B2 surface) -----------------------------------

        @Override
        public CompletableFuture<CacheWriteResponse> sendCachePut(CachePutRequest request) {
            return sendVerb(request);
        }

        @Override
        public CompletableFuture<CacheWriteResponse> sendCacheDelete(CacheDeleteRequest request) {
            return sendVerb(request);
        }

        @Override
        public CompletableFuture<CacheWriteResponse> sendCacheExpire(CacheExpireRequest request) {
            return sendVerb(request);
        }

        @Override
        public CompletableFuture<CacheReadResponse> sendCacheRead(CacheReadRequest request) {
            return sendVerb(request);
        }

        // ---- Raft verbs and reply senders (a pure cache client never issues these)

        private static UnsupportedOperationException never() {
            return new UnsupportedOperationException("ClientConnection is a pure cache client — it never sends this");
        }

        @Override
        public void sendIntroduction(Introduction introduction) {
            throw never();
        }

        @Override
        public void sendAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
            throw never();
        }

        @Override
        public CompletableFuture<StateResponse> sendStateRequest(StateRequest stateRequest) {
            throw never();
        }

        @Override
        public void sendStateResponse(StateResponse stateResponse) {
            throw never();
        }

        @Override
        public void sendVoteRequest(VoteRequest voteRequest) {
            throw never();
        }

        @Override
        public void sendVoteResponse(VoteResponse voteResponse) {
            throw never();
        }

        @Override
        public void sendPreVoteRequest(PreVoteRequest preVoteRequest) {
            throw never();
        }

        @Override
        public void sendPreVoteResponse(PreVoteResponse preVoteResponse) {
            throw never();
        }

        @Override
        public void sendAppendEntries(AppendEntries appendEntries) {
            throw never();
        }

        @Override
        public void sendAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
            throw never();
        }

        @Override
        public void sendInstallSnapshot(InstallSnapshot installSnapshot) {
            throw never();
        }

        @Override
        public void sendInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse) {
            throw never();
        }

        @Override
        public void sendCacheWriteResponse(CacheWriteResponse response) {
            throw never();
        }

        @Override
        public void sendCacheReadResponse(CacheReadResponse response) {
            throw never();
        }

        // ---- Connection state (the client is not a Raft peer: no ids, no term/index)

        @Override
        public String getRemoteNodeId() {
            return null;
        }

        @Override
        public void setRemoteNodeId(String remoteNodeId) {
            throw never();
        }

        @Override
        public NodeAddress getRemoteNodeAddress() {
            return channel.getAddress();
        }

        @Override
        public TermIndex getTermIndex() {
            return new TermIndex(0, 0L);
        }

        @Override
        public void setTermIndex(TermIndex termIndex) {
            throw never();
        }

        @Override
        public Instant getLastEntriesSent() {
            return Instant.EPOCH;
        }

        @Override
        public void setLastEntriesSent(Instant lastEntriesSent) {
            throw never();
        }

        @Override
        public Instant getLastEntriesAcknowledged() {
            return Instant.EPOCH;
        }

        @Override
        public void setLastEntriesAcknowledged(Instant lastEntriesAcknowledged) {
            throw never();
        }
    }
}
