package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class RaftNodeBehavior implements NodeCommunicationReceiver {
    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    final RaftNodeImpl self;
    final NodeStates state;
    final int term;
    private volatile boolean terminated = false;

    public RaftNodeBehavior(RaftNodeImpl self, NodeStates state, int term) {
        this.self = self;
        this.state = state;
        this.term = term;
    }

    boolean isTerminated() {
        return terminated;
    }

    void close() {
        terminated = true;
        closeInternal();
    }

    abstract void closeInternal();

    @Override
    public void onIntroduction(NodeConnectionOutbound sender, Introduction introduction) {
        // Stage 6(k) (SNAPSHOTS.md stage 6(k)): a well-formed introduction is the only
        // shape this handler acts on. The message carrier is bare, so a hostile intro can
        // carry a missing id or a missing address; both are refused here, BEFORE the
        // connection is bound, the node is registered, or a reply is sent: an unbound
        // connection and an unchanged topology, with a log -- exactly the no-reply
        // refusal shape the family uses where the handler has no ack to carry one.
        if (introduction == null
                || isInvalidNodeIdentifier(introduction.getId(), introduction.getNodeAddress())) {
            LOGGER.warn("{} Introduction from {} carries a missing node identifier ({}) - refusing; "
                            + "the connection stays unbound, nothing is registered, no topology is announced",
                    self.getId(), sender.getRemoteNodeAddress(), introduction);
            return;
        }

        // Mark the connection with the identifying node
        sender.setRemoteNodeId(introduction.getId());

        // We register data about the node that has introduced itself
        self.clusterTopology.register(new NodeIdentifier(
                introduction.getId(),
                introduction.getNodeAddress()));

        // And reply with the cluster topology as we know it
        final AnnounceClusterTopology response = new AnnounceClusterTopology(
                self.clusterTopology.getTopology().stream()
                        .map(i -> new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(
                                i.getId(),
                                i.getNodeAddress()
                        ))
                        .collect(Collectors.toList()));

        sender.sendAnnounceClusterTopology(response);
    }

    @Override
    public void onAnnounceClusterTopology(NodeConnectionOutbound sender, AnnounceClusterTopology announceClusterTopology) {
        // Stage 6(k) (SNAPSHOTS.md stage 6(k)): the announced topology is admitted as ONE
        // transaction. Both node-identifier carriers are bare, so the list can hold an
        // entry with a missing id (a null map key: legal for HashMap, but it inflates
        // getMajorityCount() and votes can never target it -- quorum DoS) or a missing
        // address (NPEs the first locate()). Refuse the WHOLE announce before registering
        // any of it, rather than registering the valid entries and throwing on the bad one.
        final List<com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier> announced =
                announceClusterTopology == null
                        ? List.of()
                        : announceClusterTopology.getNodeIdentifierStates();
        if (announceClusterTopology == null
                || announced.stream().anyMatch(i -> i == null
                        || isInvalidNodeIdentifier(i.getId(), i.getNodeAddress()))) {
            LOGGER.warn("{} AnnounceClusterTopology from {} carries a missing node identifier ({}) - refusing; "
                            + "nothing is registered and the sender stays unbound",
                    self.getId(), sender.getRemoteNodeAddress(), announceClusterTopology);
            return;
        }

        // Still unbound? The address must already be resolvable -- against what we hold or
        // what the announce lists (a node announces itself, so its own view contains its
        // own address). Checked BEFORE registering so a miss refuses the transaction
        // instead of throwing out of locate() after the topology has already moved.
        if (sender.getRemoteNodeId() == null
                && !canResolveAddress(sender.getRemoteNodeAddress(), announced)) {
            LOGGER.warn("{} AnnounceClusterTopology from unresolvable address {} - the announce does not "
                            + "name the sender and we do not hold it - refusing",
                    self.getId(), sender.getRemoteNodeAddress());
            return;
        }

        // When the topology has been received we can update our local view of the world
        self.clusterTopology.register(announced.stream()
                .map(i -> new NodeIdentifier(
                        i.getId(),
                        i.getNodeAddress()
                ))
                .collect(Collectors.toList()));

        if (sender.getRemoteNodeId() == null) {
            // canResolveAddress proved the address is present before we mutated anything,
            // so this locate is guaranteed to hit -- the throw is an invariant tripwire,
            // not a reachable refusal path.
            final String remoteNodeId = self.clusterTopology.locate(sender.getRemoteNodeAddress())
                    .map(com.github.jocull.raftcache.lib.topology.NodeIdentifier::getId)
                    .orElseThrow(() -> new IllegalStateException("Remote address " + sender.getRemoteNodeAddress() + " not found in cluster topology: {}" + self.clusterTopology.getTopology()));
            sender.setRemoteNodeId(remoteNodeId);
        }
    }

    /**
     * Stage 6(j) (SNAPSHOTS.md stage 6(j)): is this wire term/index structurally hostile —
     * {@code null}, or carrying a negative term or index? The wire {@code messages.TermIndex}
     * is a bare carrier: none of those can be constructed into it, and feeding one into a
     * domain comparison would NPE out of the wire getters or IAE out of the domain
     * constructor (the stage 6(g) root mechanism), mid-handler after any side effects the
     * accept path already performed. Every accept path checks its wire tails with this
     * BEFORE use and refuses on a hit.
     */
    static boolean isHostileWireTermIndex(com.github.jocull.raftcache.lib.raft.messages.TermIndex wire) {
        return wire == null || wire.getTerm() < 0 || wire.getIndex() < 0;
    }

    /**
     * Stage 6(k) (SNAPSHOTS.md stage 6(k)): is this wire node-identifier structurally
     * invalid — a missing (null) id, or a missing (null) address? The bootstrap carriers
     * ({@code messages.Introduction}, {@code messages.NodeIdentifier}) are bare, so —
     * unlike the term/index family, whose invalid shapes are rejected by the domain
     * constructors — a bootstrap message can reach the handler carrying either. A null id
     * becomes a NULL KEY in the topology map: legal for {@code HashMap}, but it inflates
     * {@link com.github.jocull.raftcache.lib.topology.ClusterTopology#getMajorityCount()}
     * and votes can never target the unnameable member (quorum DoS); a null address NPEs
     * the first {@code locate()} and propagates to peers in the announce replies both
     * handlers build. Every bootstrap path checks its identifiers with this BEFORE
     * binding the connection, registering, or replying.
     */
    static boolean isInvalidNodeIdentifier(String id, NodeAddress nodeAddress) {
        return id == null || nodeAddress == null;
    }

    /**
     * Stage 6(k) (SNAPSHOTS.md stage 6(k)): can {@code address} be resolved — against the
     * topology we already hold, or the announced list we are about to admit? Null-safe:
     * a null sender address and a null address inside an entry resolve to nothing. A
     * {@code true} result is what makes the post-registration {@code locate} in
     * {@link #onAnnounceClusterTopology} a reachable hit: nothing else between the check
     * and the locate can change the topology on this executor.
     */
    private boolean canResolveAddress(NodeAddress address, List<com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier> announced) {
        if (address == null) {
            return false;
        }
        boolean hit = self.clusterTopology.getTopology().stream()
                .anyMatch(i -> i.getNodeAddress() != null
                        && address.getAddress().equals(i.getNodeAddress().getAddress()));
        if (!hit) {
            hit = announced.stream()
                    .anyMatch(i -> i != null && i.getNodeAddress() != null
                            && address.getAddress().equals(i.getNodeAddress().getAddress()));
        }
        return hit;
    }

    /**
     * Raft §5.4.2 up-to-date rule evaluated against a candidate's last log entry from a
     * Pre-Vote request: grant iff the candidate's last entry is at least as up-to-date as
     * our own (higher term, or equal term and higher-or-equal index).
     *
     * <p>Pure read: the comparison is made against our CURRENT, unmutated log — exactly as
     * the vote-grant path requires (see {@code convertToFollowerForVote}). The term/index
     * comparison deliberately excludes any state mutation; pre-vote handling must never
     * change term, {@code votedFor}, or timers.
     *
     * <p>Fails closed (stage 6(i)): a hostile/missing tail is a denial, not an exception —
     * this is the single feed-in point all three pre-vote handlers use, so all of them
     * refuse such a request without any handler-side change.
     */
    boolean isUpToDate(PreVoteRequest preVoteRequest) {
        if (isHostileWireTermIndex(preVoteRequest.getLastLogTermIndex())) {
            LOGGER.warn("{} Pre-vote request from {} carries a hostile or missing last-log term/index ({}) - refusing",
                    self.getId(), preVoteRequest.getCandidateId(), preVoteRequest.getLastLogTermIndex());
            return false;
        }
        final TermIndex incomingTermIndex = new TermIndex(
                preVoteRequest.getLastLogTermIndex().getTerm(),
                preVoteRequest.getLastLogTermIndex().getIndex());
        final TermIndex currentIndex = self.logs.getCurrentTermIndex();
        LOGGER.trace("{} Pre-vote up-to-date check: candidate {} vs own log {} -> {}",
                self.getId(), incomingTermIndex, currentIndex, incomingTermIndex.compareTo(currentIndex) >= 0);
        return incomingTermIndex.compareTo(currentIndex) >= 0;
    }

    StateResponse getStateResponse(StateRequest request) {
        final TermIndex current = self.logs.getCurrentTermIndex();
        final TermIndex committed = self.logs.getCommittedTermIndex();
        // Client B §4.2 (2026-11-25): share our cluster view so a probing client
        // learns the node set to fan out StateRequest to. Empty for an unknown view.
        final List<NodeAddress> cluster = self.clusterTopology.getTopology().stream()
                .map(NodeIdentifier::getNodeAddress)
                .collect(Collectors.toList());
        return new StateResponse(
                request,
                self.getId(),
                self.getNodeAddress(),
                state,
                term,
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        current.getTerm(),
                        current.getIndex()),
                new com.github.jocull.raftcache.lib.raft.messages.TermIndex(
                        committed.getTerm(),
                        committed.getIndex()),
                cluster);
    }

    @Override
    public void onStateRequest(NodeConnectionOutbound sender, StateRequest stateRequest) {
        final StateResponse response =getStateResponse(stateRequest);
        sender.sendStateResponse(response);
    }

    /**
     * Default: an InstallSnapshot is only meaningful to a follower that accepts it (SNAPSHOTS.md
     * stage 4); other roles never act on one. Warn and ignore so an unexpected copy (e.g. a
     * leader message racing a role change) is logged, not fatal.
     */
    @Override
    public void onInstallSnapshot(NodeConnectionOutbound sender, InstallSnapshot installSnapshot) {
        LOGGER.warn("{} Ignoring InstallSnapshot from {} (term={}, lastIncluded {} @ {}) — {} does not install snapshots",
                self.getId(), sender.getRemoteNodeId(), installSnapshot.getTerm(),
                installSnapshot.getLastIncludedIndex(), installSnapshot.getLastIncludedTerm(), state);
    }

    /**
     * Default: InstallSnapshotResponse is only meaningful to the leader that sent the
     * InstallSnapshot (SNAPSHOTS.md stage 5). Ignore with a warning elsewhere.
     */
    @Override
    public void onInstallSnapshotResponse(NodeConnectionOutbound sender, InstallSnapshotResponse installSnapshotResponse) {
        LOGGER.warn("{} Ignoring InstallSnapshotResponse from {} (term={}, index={}) — {} does not track snapshot installs",
                self.getId(), sender.getRemoteNodeId(), installSnapshotResponse.getTerm(),
                installSnapshotResponse.getIndex(), state);
    }

    /**
     * Client B first cut (B §3.4, 2026-11-25): a cache write reaching a node that is not
     * the leader is a typed retryable error, not a failure — reply NOT_LEADER (and warn),
     * so the client re-discovers a leader and retries. The leader-side handler (B2)
     * overrides this and applies the verb instead.
     */
    @Override
    public void onCachePut(NodeConnectionOutbound sender, CachePutRequest request) {
        LOGGER.warn("{} CachePutRequest from {} (key={}) — {} does not serve writes; replying NOT_LEADER",
                self.getId(), sender.getRemoteNodeId(), request.getPayload().getKey(), state);
        sender.sendCacheWriteResponse(new CacheWriteResponse(request, CacheStatus.NOT_LEADER, null));
    }

    @Override
    public void onCacheDelete(NodeConnectionOutbound sender, CacheDeleteRequest request) {
        LOGGER.warn("{} CacheDeleteRequest from {} (key={}) — {} does not serve writes; replying NOT_LEADER",
                self.getId(), sender.getRemoteNodeId(), request.getPayload().getKey(), state);
        sender.sendCacheWriteResponse(new CacheWriteResponse(request, CacheStatus.NOT_LEADER, null));
    }

    @Override
    public void onCacheExpire(NodeConnectionOutbound sender, CacheExpireRequest request) {
        LOGGER.warn("{} CacheExpireRequest from {} (key={}) — {} does not serve writes; replying NOT_LEADER",
                self.getId(), sender.getRemoteNodeId(), request.getPayload().getKey(), state);
        sender.sendCacheWriteResponse(new CacheWriteResponse(request, CacheStatus.NOT_LEADER, null));
    }

    /**
     * Client B §3.4 (2026-11-25, B2): a cache read is served from this node's LOCAL
     * state-machine view when one is wired (the facade's {@code setCacheReader} seam):
     * the entry currently visible here, or not-found when the key is absent. A node with
     * no reader, or whose reader yields a view a cache handler cannot name, replies
     * not-found with a warning rather than failing the link — reads are best-effort and
     * local-serving first (B §4.3), so a follower (or a laggard view) may serve a value
     * that slightly trails a very recent leader write; that's the eventual-consistency
     * trade of LWW reads, not a defect (B §4.5).
     */
    @Override
    public void onCacheRead(NodeConnectionOutbound sender, CacheReadRequest request) {
        final Function<String, Object> reader = self.cacheReader();
        if (reader == null) {
            LOGGER.warn("{} CacheReadRequest from {} (key={}) reaches a node with no wired cache reader; replying not-found",
                    self.getId(), sender.getRemoteNodeId(), request.getKey());
            sender.sendCacheReadResponse(new CacheReadResponse(request, false, null));
            return;
        }
        final Object view = reader.apply(request.getKey());
        if (!(view instanceof KeyDataOutput output)) {
            LOGGER.warn("{} CacheReadRequest from {} (key={}) produced a view of type {} — no cache handler can serve it; replying not-found",
                    self.getId(), sender.getRemoteNodeId(), request.getKey(),
                    view == null ? "absent" : view.getClass().getName());
            sender.sendCacheReadResponse(new CacheReadResponse(request, false, null));
            return;
        }
        sender.sendCacheReadResponse(new CacheReadResponse(request, true, output));
    }
}
