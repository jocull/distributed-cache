package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeConnectionOutbound;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.messages.CachePutRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheStatus;
import com.github.jocull.raftcache.lib.raft.messages.CacheWriteResponse;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client C, stage D3 — the §4.5 acceptance bar of {@code REMOTE-CLIENT-C-LEADER-FORWARDING.md}
 * (C §5 DECISION, Redirect variant 2): the production {@code RemoteCacheClient} under leader
 * churn, over real sockets — live 3-node socket clusters ({@link ClusterKit}) for the
 * end-to-end bullets, scripted {@link ScriptedNode} stubs for the pure re-targeting choreography
 * (t2–t4) — with zero {@code RaftNode} references client-side.
 *
 * <p>What this class proves on top of B3/B4 (verbs, failover, recovery — all in
 * {@code RemoteCacheClientTest} / {@code RemoteCacheClientAcceptanceTest}):
 * <ul>
 *   <li><b>t1</b> — a write that reaches a <em>demoted</em> node commits via the
 *       NOT_LEADER reply's leader hint (Redirect) and ends up <b>applied on every node</b>;</li>
 *   <li><b>t2</b> — script-led churn mid-redirect: the verb hops A→B→C, commits under the
 *       finally-elected leader, and the SAME client's next verb goes straight to the now-
 *       confirmed cached leader with <b>no re-discovery</b>;</li>
 *   <li><b>t3</b> — a leaderless window surfaces as the TYPED, RETRYABLE
 *       {@link NoLeaderException} with <b>zero</b> put frames delivered, and the SAME client
 *       recovers once a leader exists;</li>
 *   <li><b>t4</b> — a scripted cyclic hint (A→B→C→A) terminates in a clean
 *       {@link NoLeaderException} under the per-verb anti-loop bound — every node dialed at
 *       most once, no spin;</li>
 *   <li><b>t5</b> — reads stay LOCAL-SERVING: a client that never wrote (and so never cached
 *       a leader) is served by the FIRST bootstrap node, once the seeded key has converged
 *       there — the read path has no discovery or redirect at all.</li>
 * </ul>
 */
class RemoteCacheClientRedirectAcceptanceTest {
    private static final long WAIT = 30L;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final byte[] FIELD_VALUE = "value".getBytes(StandardCharsets.UTF_8);

    private static Map<String, byte[]> data() {
        return Map.of("field", FIELD_VALUE);
    }

    private static List<NodeAddress> allAddresses(ClusterKit kit) {
        final List<NodeAddress> addresses = new ArrayList<>(kit.nodeCount());
        for (int i = 0; i < kit.nodeCount(); i++) {
            addresses.add(kit.node(i).getNodeAddress());
        }
        return addresses;
    }

    private static Throwable terminalCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static String describeChain(Throwable failure) {
        final StringBuilder sb = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (sb.length() > 0) {
                sb.append(" -> ");
            }
            sb.append(cause);
        }
        return sb.toString();
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for convergence");
        }
    }

    // ---------------------------------------------------------------------------
    // Stub — a scripted RaftNode behind its own SocketChannelMiddleware (t2–t4)
    // ---------------------------------------------------------------------------

    /**
     * A scripted {@link RaftNode} on real sockets, standing in for a cluster node whose
     * election the test does not want to run live. It plays the role {@code RaftNodeBehavior}
     * plays for a real node on the <em>server</em> of a connection: when the client dials in,
     * its middleware resolves this node from its registry (its wire address IS the middleware's
     * bound address) and calls
     * {@link #acceptConnection(ChannelMiddleware.ChannelSide)}; the node installs a receiver
     * that script-answers {@link StateRequest}s (with its current scripted state/term view)
     * and {@link CachePutRequest}s (with its scripted {@code NOT_LEADER} reply). Every reply
     * is built from the RECEIVED request, so the client's requestId-echo correlation holds.
     *
     * <p>Scripted pairs: the state/term answer for a StateRequest, and the put reply (status
     * + leader hint, where a {@code null} hint means "re-discover"). The counters let a test
     * assert the EXACT choreography a verb ran — how many dials, hint hops, and
     * re-discoveries — instead of a timing-based guess.
     *
     * <p>Everything else (election, log, connectivity) is out of scope and inert (the
     * {@code FakeRaftNode} discipline: {@code connectTo} throws rather than dials out).
     */
    static final class ScriptedNode implements RaftNode, AutoCloseable {
        /** The state/term this node reports to a StateRequest. */
        private record StateView(NodeStates state, int term) {
        }

        /** The scripted put reply: status, plus the leader hint (null = re-discover). */
        private record PutScript(CacheStatus status, NodeAddress leaderHint) {
        }

        private final String id;
        private final SocketChannelMiddleware middleware;
        private final NodeAddress address;
        private final AtomicReference<StateView> stateView =
                new AtomicReference<>(new StateView(NodeStates.FOLLOWER, 1));
        private final AtomicReference<PutScript> putScript =
                new AtomicReference<>(new PutScript(CacheStatus.NOT_LEADER, null));
        private final AtomicInteger putCount = new AtomicInteger();
        private final AtomicInteger stateRequestCount = new AtomicInteger();
        private final List<ChannelMiddleware.ChannelSide> acceptedSides = new CopyOnWriteArrayList<>();

        ScriptedNode(String id) {
            this.id = id;
            this.middleware = new SocketChannelMiddleware();
            // The node's wire address IS this middleware's bound address — the same
            // identity the client dials and the registry must resolve it by.
            this.address = middleware.getAddress();
            // Required for the server-side handshake to resolve the dialed address to this node.
            middleware.addNode(this);
        }

        NodeAddress address() {
            return address;
        }

        /** The state/term this node reports until the test re-scripts the view. */
        void setState(NodeStates state, int term) {
            stateView.set(new StateView(state, term));
        }

        /** The put reply this node gives until the test re-scripts it. */
        void scriptPutReply(CacheStatus status, NodeAddress leaderHint) {
            putScript.set(new PutScript(status, leaderHint));
        }

        int putCount() {
            return putCount.get();
        }

        int stateRequestCount() {
            return stateRequestCount.get();
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public NodeAddress getNodeAddress() {
            return address;
        }

        @Override
        public CompletableFuture<NodeConnectionOutbound> acceptConnection(ChannelMiddleware.ChannelSide incoming) {
            acceptedSides.add(incoming);
            // N1: installing the receiver completes the server half of the handshake, before
            // the middleware writes its Acknowledgement.
            incoming.setReceiver(this::onInbound);
            return CompletableFuture.completedFuture(null);
        }

        private void onInbound(Object frame) {
            if (frame instanceof StateRequest stateRequest) {
                stateRequestCount.incrementAndGet();
                final StateView view = stateView.get();
                reply(new StateResponse(stateRequest, id, address, view.state(), view.term(),
                        new com.github.jocull.raftcache.lib.raft.messages.TermIndex(view.term(), 0L),
                        new com.github.jocull.raftcache.lib.raft.messages.TermIndex(view.term(), 0L)));
            } else if (frame instanceof CachePutRequest putRequest) {
                putCount.incrementAndGet();
                final PutScript script = putScript.get();
                reply(new CacheWriteResponse(putRequest, script.status(), script.leaderHint()));
            }
            // Anything else (reads, ... ) is outside the redirect choreography under test.
        }

        /** Replies over the last accepted side (one client dials once and reuses its channel). */
        private void reply(Object response) {
            if (acceptedSides.isEmpty()) {
                throw new IllegalStateException("Received a frame before any connection was accepted");
            }
            acceptedSides.get(acceptedSides.size() - 1).send(response);
        }

        @Override
        public void close() {
            middleware.close(); // idempotent: server socket + channels
        }

        // --- Inert stubs: the redirect tests never exercise election or log semantics. ---

        @Override
        public CompletableFuture<Void> start() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stop() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<NodeStates> getNodeState() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<TermIndex> getCurrentTermIndex() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<TermIndex> getCommittedTermIndex() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<NodeIdentifier>> getKnownNodes() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<List<NodeConnectionOutbound>> connectWithTopology() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<NodeConnectionOutbound> connectTo(NodeAddress nodeAddress) {
            throw new UnsupportedOperationException("ScriptedNode does not dial out");
        }

        @Override
        public CompletableFuture<Void> disconnectFrom(NodeConnectionOutbound connection) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Optional<StateResponse>> getLeader() {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<List<StateResponse>> getClusterNodeStates() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public <T> CompletableFuture<RaftLog<T>> submit(T entry) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * A redirect that lands mid-convergence is retryable, not instantaneous (the write may
     * legally hit a still-demoting node whose answer cannot get quorum — the B4
     * {@code putEventually} shape, same bounded attempts on the SAME client).
     */
    private static void putEventually(RemoteCacheClient client, String key, int maxAttempts, long pauseMillis) {
        for (int attempt = 1; ; attempt++) {
            try {
                client.put(key, data()).join();
                return;
            } catch (RuntimeException retryable) {
                if (attempt >= maxAttempts) {
                    throw new IllegalStateException("C: put(" + key + ") did not commit within "
                            + maxAttempts + " attempts (last failure: " + describeChain(retryable) + ")", retryable);
                }
                sleepMillis(pauseMillis);
            }
        }
    }

    // ---------------------------------------------------------------------------
    // §4.5 bullet 1 — a write that reaches a demoted node redirects and applies everywhere
    // ---------------------------------------------------------------------------

    /**
     * C §4.5 (1): the client's confirmed leader is killed (two-way seal); the live pair
     * elects a fresh leader at the strictly higher term; the old leader rejoins as a
     * FOLLOWER. A client write that then reaches that demoted node is re-targeted by its
     * {@code NOT_LEADER — leader=X} reply (the C §5 Redirect fast path, or fresh discovery
     * when the hint cannot name a leader) and the entry ends up <b>applied on every node</b>
     * — not only on the one that finally served it.
     */
    @Test
    void writeReachingDemotedNodeRedirectsAndAppliesEverywhere() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode oldLeader = kit.awaitLeader("C1: no leader up after the cluster build", WAIT, UNIT);
            final int oldIdx = kit.indexOf(oldLeader);
            final int oldTerm = kit.node(oldIdx).getCurrentTermIndex().join().getTerm();

            try (RemoteCacheClient client =
                         new RemoteCacheClient(allAddresses(kit), Duration.ofSeconds(5L))) {
                client.put("warm", data()).join(); // commits before the churn; caches the confirmed leader

                kit.interrupt(oldIdx); // the confirmed leader dies (two-way seal)
                final RaftNode freshLeader = kit.awaitNewLeader(oldLeader, oldTerm,
                        "C1: the live pair did not elect after the leader died", WAIT, UNIT);
                final int freshIdx = kit.indexOf(freshLeader);
                kit.restore(oldIdx); // it rejoins as a FOLLOWER of the fresh leader
                kit.awaitSingleLeader("C1: not exactly one leader after the demoted leader rejoined", WAIT, UNIT);

                putEventually(client, "redirected", 8, 250L); // the acceptance write — via the demoted node
                final long index = kit.cache(freshIdx).getLastAppliedIndex();
                kit.awaitAppliedAll(index, "C1: the redirected write must converge on every node", WAIT, UNIT);
                for (int i = 0; i < kit.nodeCount(); i++) {
                    final KeyDataOutput view = kit.localViewOf(i, "redirected");
                    assertArrayEquals(FIELD_VALUE, view.getData().get("field"),
                            "node " + i + " must hold the redirected write byte-exact");
                }

                // The same client keeps serving end-to-end after the redirect:
                final KeyDataOutput got = client.get("redirected").join().orElseThrow();
                assertArrayEquals(FIELD_VALUE, got.getData().get("field"),
                        "read-your-own-write against the healed cluster after the redirect");
                assertFalse(client.get("absent").join().isPresent(),
                        "an absent key is a miss, not an error");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // §4.5 bullet 2 — leader churn mid-redirect: commits under the finally-elected leader
    // ---------------------------------------------------------------------------

    /**
     * C §4.5 (2): a scripted churn — A claims leadership but its put view points at B, B's
     * at C, and C commits: the verb hops A → B → C under the per-verb anti-loop bound, each
     * node dialed for the put at most once, and it commits under the finally-elected leader.
     * The SAME client's next verb then goes straight to the confirmed cached leader — no
     * re-discovery state probes, no extra put hops.
     */
    @Test
    void churnMidRedirectCommitsUnderFinallyElectedLeader() {
        try (ScriptedNode a = new ScriptedNode("A");
             ScriptedNode b = new ScriptedNode("B");
             ScriptedNode c = new ScriptedNode("C")) {
            // Phase 1 — discovery picks A (the LEADER claim at the highest term); the put
            // view chains A(refuse→B) → B(refuse→C) → C(commit).
            a.setState(NodeStates.LEADER, 1);
            b.setState(NodeStates.FOLLOWER, 1);
            c.setState(NodeStates.FOLLOWER, 1);
            a.scriptPutReply(CacheStatus.NOT_LEADER, b.address());
            b.scriptPutReply(CacheStatus.NOT_LEADER, c.address());
            c.scriptPutReply(CacheStatus.OK, null);

            try (RemoteCacheClient client =
                         new RemoteCacheClient(List.of(a.address(), b.address(), c.address()),
                                 Duration.ofSeconds(5L))) {
                client.put("churn", data()).join();
                assertEquals(1, a.putCount(), "A must receive the verb exactly once (anti-loop bound)");
                assertEquals(1, b.putCount(), "B must receive the verb exactly once (anti-loop bound)");
                assertEquals(1, c.putCount(), "C must commit the verb under churn");

                // Phase 2 — the election settles on C (term 2); the client's confirmed
                // leader is already C (set by the chained redirect), so this verb lands
                // directly with no discovery.
                a.setState(NodeStates.FOLLOWER, 2);
                b.setState(NodeStates.FOLLOWER, 2);
                c.setState(NodeStates.LEADER, 2);

                client.put("settled", data()).join();
                assertEquals(1, a.putCount(), "A must receive no further puts after the churn settled on C");
                assertEquals(1, b.putCount(), "B must receive no further puts after the churn settled on C");
                assertEquals(2, c.putCount(), "C (the finally-elected leader) must commit both verbs");
            }

            // No re-discovery ever: each node's StateRequest happened exactly once (phase 1).
            assertEquals(1, a.stateRequestCount(), "A was probed once — the phase-2 verb used the confirmed leader, not discovery");
            assertEquals(1, b.stateRequestCount(), "B was probed once — the phase-2 verb used the confirmed leader, not discovery");
            assertEquals(1, c.stateRequestCount(), "C was probed once — the phase-2 verb used the confirmed leader, not discovery");
        }
    }

    // ---------------------------------------------------------------------------
    // §4.5 bullet 3 — a leaderless window: typed retryable failure, then recovery
    // ---------------------------------------------------------------------------

    /**
     * C §4.5 (3): every node reports FOLLOWER — a leaderless window. The client's write
     * fails with the TYPED, retryable {@link NoLeaderException} at the DISCOVERY level:
     * zero put frames are delivered to any node. When a leader does emerge, the SAME client
     * recovers on its very next verb (one bounded re-discovery) and commits — proving the
     * failure is retryable, not terminal.
     */
    @Test
    void noLeaderWindowIsTypedRetryableAndRecovers() {
        try (ScriptedNode a = new ScriptedNode("A");
             ScriptedNode b = new ScriptedNode("B");
             ScriptedNode c = new ScriptedNode("C")) {
            a.setState(NodeStates.FOLLOWER, 1);
            b.setState(NodeStates.FOLLOWER, 1);
            c.setState(NodeStates.FOLLOWER, 1); // nobody claims leadership

            try (RemoteCacheClient client =
                         new RemoteCacheClient(List.of(a.address(), b.address(), c.address()),
                                 Duration.ofSeconds(5L))) {
                final RuntimeException dead = assertThrows(RuntimeException.class,
                        () -> client.put("window", data()).join());
                assertEquals(NoLeaderException.class, terminalCause(dead).getClass(),
                        "a leaderless window must be the typed NoLeaderException, was: " + describeChain(dead));
                assertTrue(terminalCause(dead).getMessage().contains("reported LEADER"),
                        "the discovery-level verdict must name the absence of a leader report, was: "
                                + terminalCause(dead).getMessage());

                assertEquals(0, a.putCount(), "no put may be delivered while no leader reports at all");
                assertEquals(0, b.putCount(), "no put may be delivered while no leader reports at all");
                assertEquals(0, c.putCount(), "no put may be delivered while no leader reports at all");
                assertEquals(1, a.stateRequestCount(), "A was probed exactly once by the failed discovery");
                assertEquals(1, b.stateRequestCount(), "B was probed exactly once by the failed discovery");
                assertEquals(1, c.stateRequestCount(), "C was probed exactly once by the failed discovery");

                // A leader emerges: the same client must recover on its next verb.
                a.setState(NodeStates.LEADER, 2);
                a.scriptPutReply(CacheStatus.OK, null);

                client.put("window", data()).join();
                assertEquals(1, a.putCount(), "A must commit the post-recovery verb");
                assertEquals(0, b.putCount(), "B receives no puts — A is the confirmed leader");
                assertEquals(0, c.putCount(), "C receives no puts — A is the confirmed leader");
                assertEquals(2, a.stateRequestCount(), "A saw the failed discovery and the recovery discovery");
                assertEquals(2, b.stateRequestCount(), "B saw the failed discovery and the recovery discovery");
                assertEquals(2, c.stateRequestCount(), "C saw the failed discovery and the recovery discovery");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // §4.5 bullet 4 — cyclic hints: anti-loop bound, clean typed termination
    // ---------------------------------------------------------------------------

    /**
     * C §4.5 (4): a scripted cyclic hint — A points at B, B at C, C at A — is the
     * pathological case a Redirect implementation must bound. The client may re-discover
     * once after the cycle closes, but the refreshed A is already on the per-verb refused
     * set, so the verb must terminate in a CLEAN, retryable {@link NoLeaderException} (the
     * anti-loop verdict, not the discovery-level one): every node dialed for the put
     * exactly once, discovery probed exactly twice (initial + the single bounded
     * re-discovery), and no spin.
     */
    @Test
    void cyclicHintsTerminateInCleanClientError() {
        try (ScriptedNode a = new ScriptedNode("A");
             ScriptedNode b = new ScriptedNode("B");
             ScriptedNode c = new ScriptedNode("C")) {
            a.setState(NodeStates.LEADER, 1); // discovery picks A — it claims leadership, yet...
            b.setState(NodeStates.FOLLOWER, 1);
            c.setState(NodeStates.FOLLOWER, 1);
            a.scriptPutReply(CacheStatus.NOT_LEADER, b.address());
            b.scriptPutReply(CacheStatus.NOT_LEADER, c.address());
            c.scriptPutReply(CacheStatus.NOT_LEADER, a.address()); // ...points back: the cycle

            try (RemoteCacheClient client =
                         new RemoteCacheClient(List.of(a.address(), b.address(), c.address()),
                                 Duration.ofSeconds(5L))) {
                final RuntimeException dead = assertThrows(RuntimeException.class,
                        () -> client.put("cycle", data()).join());
                final Throwable terminal = terminalCause(dead);
                assertEquals(NoLeaderException.class, terminal.getClass(),
                        "cyclic hints must terminate in the typed NoLeaderException, was: " + describeChain(dead));
                final String message = terminal.getMessage();
                assertTrue(message.contains("willing to lead"),
                        "the anti-loop verdict must say the named nodes were not willing to lead, was: " + message);
                assertFalse(message.contains("reported LEADER"),
                        "this is the anti-loop verdict, not the discovery-level no-leader verdict, was: " + message);

                assertEquals(1, a.putCount(), "A must be dialed for the put at most once (anti-loop bound)");
                assertEquals(1, b.putCount(), "B must be dialed for the put at most once (anti-loop bound)");
                assertEquals(1, c.putCount(), "C must be dialed for the put at most once (anti-loop bound)");
                assertEquals(2, a.stateRequestCount(), "A saw the initial discovery plus exactly one bounded re-discovery");
                assertEquals(2, b.stateRequestCount(), "B saw the initial discovery plus exactly one bounded re-discovery");
                assertEquals(2, c.stateRequestCount(), "C saw the initial discovery plus exactly one bounded re-discovery");
            }
        }
    }

    // ---------------------------------------------------------------------------
    // §4.5 bullet 5 — reads stay local-serving
    // ---------------------------------------------------------------------------

    /**
     * C §4.5 (5): a client that has ONLY read never caches a leader, and its reads are served
     * by the FIRST bootstrap node — no discovery, no leader routing. With a follower first in
     * the bootstrap order, a seeded key that has merely CONVERGED (local-serving is the
     * follower's job) already reads back byte-exact, and a miss is a miss — a read never
     * touches the leader path.
     */
    @Test
    void readsStayLocalServingWithoutLeaderRouting() {
        try (ClusterKit kit = new ClusterKit()) {
            final RaftNode leader = kit.awaitLeader("C5: no leader up after the cluster build", WAIT, UNIT);
            final int leaderIdx = kit.indexOf(leader);

            // Pick a FOLLOWER and put it FIRST in the bootstrap order — if reads were leader-
            // routed, the first node to serve them would be the leader, not this follower.
            final int f = (leaderIdx + 1) % kit.nodeCount();
            final List<NodeAddress> followerFirst = new ArrayList<>(allAddresses(kit));
            followerFirst.add(0, followerFirst.remove(kit.indexOf(kit.node(f)))); // f -> position 0

            // Seed through the cache layer itself (client C is read-only here): the value lands
            // on the leader, then converges to the follower that will serve the read.
            kit.cache(leaderIdx).put("seed", data()).join();
            final long deadline = System.currentTimeMillis() + UNIT.toMillis(WAIT);
            while (!kit.localHas(f, "seed")) {
                assertTrue(System.currentTimeMillis() < deadline,
                        "the seeded key never converged to follower " + f);
                sleepMillis(25L);
            }

            try (RemoteCacheClient client =
                         new RemoteCacheClient(followerFirst, Duration.ofSeconds(5L))) {
                final KeyDataOutput got = client.get("seed").join().orElseThrow(
                        () -> new AssertionError("the seeded key must read back from the first bootstrap node (the follower)"));
                assertEquals("seed", got.getKey(), "the read must name the key it served");
                assertArrayEquals(FIELD_VALUE, got.getData().get("field"),
                        "the follower must serve the converged value byte-exact — local-serving, not leader-fetching");
                assertTrue(client.get("absent").join().isEmpty(),
                        "an absent key reads as a clean miss from the local follower");
            }
        }
    }

}
