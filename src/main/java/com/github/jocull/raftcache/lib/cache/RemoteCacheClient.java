package com.github.jocull.raftcache.lib.cache;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.middleware.SocketChannelMiddleware;
import com.github.jocull.raftcache.lib.raft.messages.CacheDeleteRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheExpireRequest;
import com.github.jocull.raftcache.lib.raft.messages.CachePutRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadRequest;
import com.github.jocull.raftcache.lib.raft.messages.CacheReadResponse;
import com.github.jocull.raftcache.lib.raft.messages.CacheStatus;
import com.github.jocull.raftcache.lib.raft.messages.CacheWriteResponse;
import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.Response;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The REMOTE client for the distributed cache (client B, {@code REMOTE-CLIENT-B-CLIENT-RPC.md}):
 * the same verb surface as the in-process {@link DistributedCache} facade
 * ({@link #put}, {@link #delete}, {@link #expire}, {@link #get}), delivered over real sockets —
 * and holding <em>no</em> {@code RaftNode} reference anywhere (acceptance: "real client over
 * sockets, no RaftNode ref"). The client speaks the frozen wire POJOs of
 * {@code lib.raft.messages} (envelopes 15–20 plus the pre-existing {@code StateRequest}/
 * {@code StateResponse}) directly over a {@link SocketChannelMiddleware} channel (B §4.1: the
 * first frame may be a client verb — no Introduction).
 *
 * <h3>Construction</h3>
 * <p>Configured with a <b>bootstrap node list</b> (B §3.5): the dialable {@code host:port}s the
 * client can reach up front; they may be a <em>partial</em> view of the cluster — discovery
 * widens it. The client owns one {@link SocketChannelMiddleware} purely as a dial fabric: it
 * binds an ephemeral socket on {@code 127.0.0.1} for the client's own identity and parks an
 * idle daemon acceptor (nothing dials back at a client); every thread is a daemon so a JVM can
 * always exit. {@link #close()} tears the fabric and every channel down and fails any in-flight
 * request.
 *
 * <h3>Leader discovery (B §4.2 decision, 2026-11-25)</h3>
 * <p>Hybrid topology discovery: fan out {@code StateRequest} over the bootstrap list, UNION
 * the cluster views the replies report (the extended {@code StateResponse.cluster}), fan out
 * again over that union, and pick the LEADER (tie-break: highest term). The discovered leader
 * is cached and reused; any NOT_LEADER reply or any dial failure to it abandons the claim —
 * the SAME verb may re-target straight to the leader the NOT_LEADER reply names (Redirect,
 * C §5 decision, 2026-11-25) before a subsequent verb re-discovers.
 *
 * <h3>Error policy (B doc §5 acceptance)</h3>
 * <ul>
 *   <li><b>no leader</b> (all probed views lack a LEADER) ⇒ {@link NoLeaderException} — a
 *       clean, TYPED, RETRYABLE error that mirrors the facade's {@code IllegalStateException}
 *       refusal; a subsequent attempt re-discovers (bounded: every attempt re-probes, no
 *       unbounded loop).</li>
 *   <li><b>NOT_LEADER write reply</b> ⇒ re-target straight to the leader the reply NAMES
 *       (the Redirect hint, C §5 decision 2026-11-25); when it names none — or that node
 *       was already refused — one re-discovery + ONE retry on the fresh leader (B §3.5
 *       bounded retry / anti-loop: no node dialed twice per verb call, so a scripted
 *       not-leader loop terminates in a clean {@link NoLeaderException}).</li>
 *   <li><b>unresponsive peer</b> ⇒ the per-request timeout (B §4.6 decision, 2026-11-25):
 *       every request is bounded by {@code requestTimeout} via {@link
 *       CompletableFuture#orTimeout}, so a silent peer surfaces as a typed
 *       {@link java.util.concurrent.TimeoutException}, not a hang — as the <em>terminal</em>
 *       cause of the failed future (it is checked, and completion-chain lambdas may only
 *       rethrow unchecked exceptions, so the terminal is carried in an unchecked wrapper
 *       whose cause chain ends at the {@code TimeoutException}). The NodeRequests 5 s peer
 *       cancel (in-raft) is a different path and stays untouched.</li>
 * </ul>
 *
 * <h3>Write &amp; read semantics (B §4.3 / §4.5 decisions)</h3>
 * <p>Writes go to the current leader and complete only after it has ACKed — which the leader
 * behavior does only after commit by a majority <em>and</em> leader-local apply (apply-then-
 * acknowledge), so a completed write is durable and <b>read-your-own-write holds</b> the moment
 * the future completes. The cache is last-writer-wins by key with <b>no dedup table</b>: a
 * failed-then-retried write re-applies the same key→value and converges (documented beneficial
 * property). Reads are <b>local-serving</b>: served from whatever node answers — the leader
 * when one is known (read-your-own-write preference), otherwise a bootstrap node — and an
 * absent (or expired) key is a miss, not an error.
 */
public final class RemoteCacheClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteCacheClient.class);

    /** B §4.6 (2026-11-25): the client-request path's default per-request timeout. */
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final List<NodeAddress> bootstrap;
    private final Duration requestTimeout;
    /** Dial fabric only (see class javadoc): binds the client's identity socket and dials. */
    private final SocketChannelMiddleware channelFactory;
    /** The handshake's client identity: the dial fabric's bound address. */
    private final NodeAddress clientAddress;
    /** The client's live view of the cluster: bootstrap ∪ every reported {@code cluster} view. */
    private final Set<NodeAddress> knownNodes;
    /** One dialed side per target address (handshake done, receiver installed). */
    private final Map<NodeAddress, ChannelMiddleware.ChannelSide> channels = new ConcurrentHashMap<>();
    /**
     * Every in-flight request, keyed by its wire {@code requestId}; the reply frame (which the
     * server echoes by id) removes and completes it — the correlation book-keeping every sender
     * needs, without reusing the in-raft {@code NodeRequests} machinery.
     */
    private final Map<UUID, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
    /** The leader the client last confirmed (abandoned on NOT_LEADER / dial failure). */
    private final java.util.concurrent.atomic.AtomicReference<NodeAddress> currentLeader =
            new java.util.concurrent.atomic.AtomicReference<>();
    private volatile boolean closed = false;

    /**
     * @param bootstrap the dialable {@code host:port}s of one or more cluster nodes — a partial
     *                  list is fine, discovery widens it (B §4.2)
     * @throws NullPointerException     if {@code bootstrap} or any address is null
     * @throws IllegalArgumentException if {@code bootstrap} is empty
     */
    public RemoteCacheClient(List<NodeAddress> bootstrap) {
        this(bootstrap, DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * @param bootstrap     as {@link #RemoteCacheClient(List)}
     * @param requestTimeout the per-request budget applied on the client's sender path (B §4.6);
     *                       an unresponsive peer then fails the request with a typed
     *                       {@link java.util.concurrent.TimeoutException} instead of hanging
     * @throws NullPointerException     if {@code bootstrap} or any address is null
     * @throws IllegalArgumentException if {@code bootstrap} is empty or {@code requestTimeout}
     *                                  is null, zero or negative
     */
    public RemoteCacheClient(List<NodeAddress> bootstrap, Duration requestTimeout) {
        Objects.requireNonNull(bootstrap, "bootstrap must not be null");
        Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        if (bootstrap.isEmpty()) {
            throw new IllegalArgumentException("bootstrap must not be empty");
        }
        for (final NodeAddress address : bootstrap) {
            Objects.requireNonNull(address, "bootstrap addresses must not be null");
        }
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        this.bootstrap = List.copyOf(bootstrap);
        this.requestTimeout = requestTimeout;
        this.channelFactory = new SocketChannelMiddleware();
        this.clientAddress = channelFactory.getAddress();
        this.knownNodes = ConcurrentHashMap.newKeySet();
        knownNodes.addAll(bootstrap);
    }

    // ---------------------------------------------------------------------------
    // Verb API — mirrors the DistributedCache facade (client B §3.5)
    // ---------------------------------------------------------------------------

    /**
     * Puts {@code data} under {@code key}, routed to the current leader (discovered if none is
     * known yet). The returned future completes only after the entry has been committed by a
     * majority AND applied on the leader (apply-then-acknowledge) — a completed
     * put is durable and immediately readable (read-your-own-write).
     *
     * <p>Failure modes (all retryable, see class javadoc): {@link NoLeaderException} when
     * discovery finds no leader; {@link java.util.concurrent.TimeoutException} when a peer is
     * silent past {@code requestTimeout}; {@link IllegalStateException} on a refused write.
     */
    public CompletableFuture<Void> put(String key, Map<String, byte[]> data) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        return write(new CachePutRequest(new KeyDataInput(key, data)));
    }

    /**
     * Deletes {@code key}, routed to the current leader. Deleting an absent key is a no-op at
     * the state machine — the future still completes normally (retries are safe).
     */
    public CompletableFuture<Void> delete(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return write(new CacheDeleteRequest(new KeyDataDeleteInput(key)));
    }

    /**
     * Sets the expiration of {@code key} to "now + {@code ttl}", routed to the current leader.
     * "Now" resolves at apply time on each node (per-node clock); an EXPIRE on an absent key is
     * a no-op — the future still completes normally and the key is never resurrected.
     */
    public CompletableFuture<Void> expire(String key, Duration ttl) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        Objects.requireNonNull(ttl, "ttl must not be null");
        return write(new CacheExpireRequest(new KeyDataExpireInput(key, ttl)));
    }

    /**
     * Reads {@code key}. Local-serving (B §4.3): served from whichever node answers — the
     * leader when one is known (read-your-own-write preference), otherwise the first
     * bootstrap node. An absent or already-expired key is an EMPTY optional, not an error.
     */
    public CompletableFuture<Optional<KeyDataOutput>> get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        final NodeAddress target = currentLeader.get() != null ? currentLeader.get() : bootstrap.get(0);
        return sendTo(target, new CacheReadRequest(key)).handle((response, failure) -> {
            if (failure != null) {
                throw unwrap(failure);
            }
            final CacheReadResponse reply = (CacheReadResponse) response;
            return reply.isFound() ? Optional.ofNullable(reply.getValue()) : Optional.empty();
        });
    }

    // ---------------------------------------------------------------------------
    // Discovery & write machinery
    // ---------------------------------------------------------------------------

    /**
     * A confirmed leader, or a fresh discovery (B §4.2): probe the bootstrap list, UNION the
     * reported cluster views, probe the union, pick the LEADER by highest term. Runs off the
     * caller thread (the fan-out dials and waits, bounded by {@code requestTimeout} per peer).
     *
     * @throws NoLeaderException (in the returned future) when no probed node reports LEADER
     */
    private CompletableFuture<NodeAddress> ensureLeader() {
        checkOpen();
        final NodeAddress cached = currentLeader.get();
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }
        return CompletableFuture.supplyAsync(this::discoverLeaderOrFail);
    }

    private NodeAddress discoverLeaderOrFail() {
        final Map<NodeAddress, StateResponse> replies = new HashMap<>();
        probe(bootstrap, replies);
        final Set<NodeAddress> union = new java.util.LinkedHashSet<>(replies.keySet());
        replies.values().forEach(reply -> union.addAll(reply.getCluster()));
        probe(union, replies);

        NodeAddress leader = null;
        int bestTerm = Integer.MIN_VALUE;
        for (final StateResponse reply : replies.values()) {
            if (reply.getState() == NodeStates.LEADER && reply.getTerm() > bestTerm) {
                bestTerm = reply.getTerm();
                leader = reply.getIdentifier().getNodeAddress();
            }
        }
        if (leader == null) {
            throw new NoLeaderException("No node in " + union + " reported LEADER (replies from "
                    + replies.keySet() + "); a leader may be electing — retry");
        }
        knownNodes.addAll(replies.keySet());
        currentLeader.set(leader);
        return leader;
    }

    /**
     * Fan {@code StateRequest}s out over {@code targets}, harvesting the replies into {@code
     * replies} (already-answered targets are skipped). A target that fails its per-request
     * timeout or refuses the dial is skipped — discovery keeps probing rather than aborting.
     */
    private void probe(Collection<? extends NodeAddress> targets, Map<NodeAddress, StateResponse> replies) {
        for (final NodeAddress target : targets) {
            if (replies.containsKey(target)) {
                continue;
            }
            try {
                final StateResponse reply = (StateResponse) sendTo(target, new StateRequest()).join();
                replies.put(target, reply);
                knownNodes.addAll(reply.getCluster());
            } catch (RuntimeException skip) {
                LOGGER.debug("Discovery probe to {} failed: {}", target.getAddress(), skip.toString());
            }
        }
    }

    /**
     * Writes one verb (C §5 DECISION, 2026-11-25 — Redirect variant): to the current leader
     * (discovered if none is confirmed yet), and on a NOT_LEADER reply FIRST re-targeting
     * straight to the leader the reply names (§5 fast path), else falling back to a fresh
     * discovery (the B §3.5 shape). A verb call tries each distinct node at most once
     * (anti-loop), so a pathological not-leader loop terminates in a clean
     * {@link NoLeaderException} rather than spinning.
     */
    private CompletableFuture<Void> write(Request writeRequest) {
        return ensureLeader()
                .thenCompose(initial -> attemptWrite(writeRequest, initial, new HashSet<>()));
    }

    /**
     * One hop of {@link #write(Request)}: send the verb to {@code target}; on a NOT_LEADER
     * reply abandon the leadership claim, then take the Redirect fast path (re-target straight
     * to the reply's leader hint) when it names a node this verb call has not already refused,
     * else one re-discovery plus retry on the fresh leader. Any other refusal is a terminal
     * {@link IllegalStateException}.
     *
     * <p>{@code refused} is this verb call's anti-loop bound (B §3.5): the hint branch and the
     * discovery branch both check it, so no node is ever dialed twice by one verb call and the
     * redirect chain terminates.
     */
    private CompletableFuture<Void> attemptWrite(Request writeRequest, NodeAddress target, Set<NodeAddress> refused) {
        refused.add(target);
        return sendTo(target, writeRequest).thenCompose(response -> {
            final CacheWriteResponse reply = (CacheWriteResponse) response;
            if (reply.getStatus() == CacheStatus.OK) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            abandonLeaderIf(target);
            if (reply.getStatus() != CacheStatus.NOT_LEADER) {
                return CompletableFuture.<Void>failedFuture(new IllegalStateException(
                        "Node " + target + " refused the write: " + reply.getStatus()));
            }
            final NodeAddress hint = reply.getLeaderHint();
            if (hint != null && !refused.contains(hint)) {
                currentLeader.set(hint);
                return attemptWrite(writeRequest, hint, refused);
            }
            return ensureLeader().thenCompose(fresh -> {
                if (refused.contains(fresh)) {
                    return CompletableFuture.<Void>failedFuture(new NoLeaderException(
                            "Every node this verb named or refused was not willing to lead (tried "
                                    + refused + "); a leader may be electing — retry"));
                }
                return attemptWrite(writeRequest, fresh, refused);
            });
        }).exceptionally(failure -> {
            abandonLeaderIf(target);
            throw unwrap(failure);
        });
    }

    /**
     * Sends one request over (dialing if needed) the side to {@code target} and returns its
     * reply future bounded by {@code requestTimeout} (B §4.6). Correlation is by the request's
     * wire id: the reply the server echoes removes the waiter — a late-or-timed-out reply is
     * dropped, not mis-delivered.
     */
    private CompletableFuture<Response> sendTo(NodeAddress target, Request request) {
        checkOpen();
        Objects.requireNonNull(request, "request must not be null");
        final CompletableFuture<Object> waiter = new CompletableFuture<>();
        final UUID requestId = request.getRequestId();
        pending.put(requestId, waiter);
        // Identity-checked eviction: a Redirect hop (C §5) re-sends the SAME request id under a
        // fresh waiter, and CompletableFuture fires dependents in LIFO order — so the earlier
        // hop's cleanup runs AFTER the orTimeout relay has already executed the hop that
        // re-registered the id. An unguarded remove here would evict the NEW hop's waiter.
        waiter.whenComplete((ignored, failure) -> pending.remove(requestId, waiter));
        try {
            sideTo(target).send(request);
        } catch (RuntimeException dialFailure) {
            pending.remove(requestId);
            abandonLeaderIf(target);
            throw dialFailure;
        }
        @SuppressWarnings("unchecked")
        final CompletableFuture<Response> timed = (CompletableFuture<Response>) (CompletableFuture<?>)
                waiter.orTimeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        return timed;
    }

    /**
     * The dialed side to {@code target}, dialing once per address (handshake then receiver
     * install — {@code openChannel} returns only after the peer's receiver exists, framing-
     * safe both ways).
     */
    private ChannelMiddleware.ChannelSide sideTo(NodeAddress target) {
        Objects.requireNonNull(target, "target must not be null");
        synchronized (channels) {
            final ChannelMiddleware.ChannelSide side = channels.get(target);
            if (side == null) {
                final ChannelMiddleware.ChannelSide dialed = channelFactory.openChannel(clientAddress, target);
                dialed.setReceiver(this::onInbound);
                channels.put(target, dialed);
                return dialed;
            }
            return side;
        }
    }

    /** Reply routing (shared by every dialed side): any response POJO completes its waiter. */
    private void onInbound(Object frame) {
        if (frame instanceof Response response) {
            final CompletableFuture<Object> waiter = pending.remove(response.getRequestId());
            if (waiter == null) {
                LOGGER.debug("Dropping late reply {} (request {})",
                        response.getClass().getSimpleName(), response.getRequestId());
                return;
            }
            waiter.complete(response);
        } else {
            LOGGER.debug("Ignoring non-response frame {}", frame.getClass().getSimpleName());
        }
    }

    private void abandonLeaderIf(NodeAddress maybeLeader) {
        if (Objects.equals(maybeLeader, currentLeader.get())) {
            currentLeader.set(null);
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("client is closed");
        }
    }

    /**
     * Unwrap the transport wrapping so the typed cause travels with the failure: an unchecked
     * cause ({@link NoLeaderException}, a dial {@link IllegalStateException}, …) is rethrown
     * as-is; a checked cause — the {@code orTimeout}'s {@link java.util.concurrent.TimeoutException} —
     * cannot be thrown from the completion lambdas, so it is carried as the <em>terminal</em>
     * cause of an unchecked wrapper instead. Callers walk {@code getCause()} to the terminal
     * and find the typed error either way.
     */
    private static RuntimeException unwrap(Throwable failure) {
        Throwable cause = failure;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof RuntimeException re) {
            return re;
        }
        return new IllegalStateException("Request failed: " + cause, cause);
    }

    // ---------------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------------

    /**
     * Fails every in-flight request with {@link IllegalStateException}, closes every dialed
     * channel and the dial fabric. Idempotent.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        final List<CompletableFuture<Object>> waiters = new ArrayList<>(pending.values());
        pending.clear();
        for (final CompletableFuture<Object> waiter : waiters) {
            waiter.completeExceptionally(new IllegalStateException("client closed"));
        }
        for (final ChannelMiddleware.ChannelSide side : channels.values()) {
            side.close();
        }
        channels.clear();
        channelFactory.close();
    }
}
