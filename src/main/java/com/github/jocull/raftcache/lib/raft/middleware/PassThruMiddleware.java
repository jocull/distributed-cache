package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A pass-thru object middleware, great for testing "networks" in memory to validate
 * communication strategies between objects. Adds random delays between calls to simulate
 * a fairly low latency network.
 */
public class PassThruMiddleware implements ChannelMiddleware, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PassThruMiddleware.class);
    private final Random random = new Random();
    private final Map<NodeAddress, RaftNode> addressRaftNodeMap = new HashMap<>();
    private final List<PassThruPair> connections = new ArrayList<>();
    private final Deque<CompletableFuture<?>> futures = new ArrayDeque<>();

    // Per-node fault injection, READ on every send/receive hop from unrelated executor
    // threads, so the map itself must be thread-safe. Values are immutable and swapped
    // atomically by the synchronized setters below. Absent (or NONE) == "healthy node" and
    // reproduces the previous baseline behaviour (1..9 ms per hop, no loss).
    private final Map<NodeAddress, NodeFault> nodeFaults = new ConcurrentHashMap<>();

    @Override
    public void close() {
        synchronized (futures) {
            futures.forEach(f -> f.cancel(false));
            futures.clear();
        }
    }

    public Map<NodeAddress, RaftNode> getAddressRaftNodeMap() {
        return Map.copyOf(addressRaftNodeMap);
    }

    public synchronized PassThruMiddleware addNode(RaftNode raftNode) {
        Objects.requireNonNull(raftNode, "raftNode must not be null");
        addressRaftNodeMap.compute(raftNode.getNodeAddress(), (k, v) -> {
            if (v != null) {
                throw new IllegalArgumentException("Node " + v.getId() + " already registered!");
            }
            return raftNode;
        });
        return this;
    }

    public synchronized boolean removeNode(RaftNode raftNode) {
        return addressRaftNodeMap.remove(raftNode.getNodeAddress()) != null;
    }

    // ---------------------------------------------------------------------------
    // Per-node fault injection (enables flaky-node / slow-node scenarios in tests).
    //
    // A node's fault is applied to every hop its side participates in: send() applies
    // the SENDER's profile, receive() applies the RECEIVER's profile. So a "slow" node
    // (latencyFloorMillis > 0) makes both its outbound and inbound hops slower, and a
    // "flaky" node (packetLossRate > 0) can drop both what it sends and what it receives.
    // The baseline 1..9 ms per-hop delay and per-hop reorder are always kept, so a fault
    // only ADDS latency/loss on top of the existing network.
    // ---------------------------------------------------------------------------

    /**
     * Make every hop touching {@code nodeAddress} slower by at least {@code extraLatencyMillis}
     * (added on top of the baseline 1..9 ms per hop). {@code 0} resets to the baseline for
     * latency (packet-loss is left unchanged).
     */
    public synchronized PassThruMiddleware setNodeLatency(NodeAddress nodeAddress, long extraLatencyMillis) {
        Objects.requireNonNull(nodeAddress, "nodeAddress must not be null");
        if (extraLatencyMillis < 0) {
            throw new IllegalArgumentException("extraLatencyMillis must be >= 0, got " + extraLatencyMillis);
        }
        nodeFaults.compute(nodeAddress, (k, f) -> {
            final NodeFault updated = new NodeFault(extraLatencyMillis, f != null ? f.packetLossRate : 0.0);
            return updated.isNone() ? null : updated;
        });
        return this;
    }

    /**
     * Make each hop touching {@code nodeAddress} drop the message with probability
     * {@code packetLossRate} (must be in {@code [0,1]}). Independent per hop, mirroring the
     * existing per-hop delay. {@code 0} resets to no loss (latency is left unchanged).
     */
    public synchronized PassThruMiddleware setNodePacketLoss(NodeAddress nodeAddress, double packetLossRate) {
        Objects.requireNonNull(nodeAddress, "nodeAddress must not be null");
        if (packetLossRate < 0.0 || packetLossRate > 1.0) {
            throw new IllegalArgumentException("packetLossRate must be in [0,1], got " + packetLossRate);
        }
        nodeFaults.compute(nodeAddress, (k, f) -> {
            final NodeFault updated = new NodeFault(f != null ? f.latencyFloorMillis : 0L, packetLossRate);
            return updated.isNone() ? null : updated;
        });
        return this;
    }

    /** Remove any injected fault (latency and loss) for {@code nodeAddress}. */
    public synchronized PassThruMiddleware clearNodeFaults(NodeAddress nodeAddress) {
        Objects.requireNonNull(nodeAddress, "nodeAddress must not be null");
        nodeFaults.remove(nodeAddress);
        return this;
    }

    /** Remove injected faults for every node. */
    public synchronized PassThruMiddleware clearAllNodeFaults() {
        nodeFaults.clear();
        return this;
    }

    private NodeFault profile(NodeAddress nodeAddress) {
        final NodeFault f = nodeFaults.get(nodeAddress);
        return f != null ? f : NodeFault.NONE;
    }

    private void addAndCleanFuture(CompletableFuture<?> future) {
        synchronized (futures) {
            futures.removeIf(CompletableFuture::isDone);
            futures.add(future);
        }
    }

    // Isolation is per-NODE, not per-PAIR. A "side" belongs to the node whose RaftNode
    // it wraps; send() checks the SENDER's own side and receive() checks the RECEIVER's
    // own side, so a message crosses a pair only while NEITHER endpoint is down. Hence
    // "seal a node" must flip ONLY that node's own side on every pair it touches, and
    // never a neighbour's side.
    //
    // The pre-fix code ran Pair.interrupt()/Pair.restore(), which set/cleared BOTH sides
    // of a pair. That meant restoring node X silently cleared the still-sealed side of a
    // neighbour Y sharing the pair, re-opening the link even though Y was isolated. The
    // (zombie) leader could then resume retransmitting and re-feed the very node a test
    // had isolated, defeating partition-based regression pins (Decision #1). Interrupting
    // a node still closes every link touching it in both directions; only the spurious
    // cross-node side flip on restore is gone.
    public void interrupt(NodeAddress nodeAddress) {
        synchronized (connections) {
            connections.forEach(c -> c.interruptFor(nodeAddress));
        }
    }

    public void restore(NodeAddress nodeAddress) {
        synchronized (connections) {
            connections.forEach(c -> c.restoreFor(nodeAddress));
        }
    }

    @Override
    public ChannelMiddleware.ChannelSide openChannel(RaftNode source, NodeAddress targetAddress) {
        final RaftNode target = addressRaftNodeMap.get(targetAddress);
        if (target == null) {
            throw new IllegalStateException("Target node " + targetAddress.getAddress() + " was not registered!");
        }
        final PassThruPair pair = new PassThruPair(source, target);

        target.acceptConnection(pair.getRight()).join(); // Let the connection establish first
        synchronized (connections) {
            connections.add(pair);
        }
        return pair.getLeft();
    }

    private class PassThruPair implements ChannelMiddleware.ChannelPair {
        private final PassThruChannelSide left;
        private final PassThruChannelSide right;

        public PassThruPair(RaftNode leftNode, RaftNode rightNode) {
            left = new PassThruChannelSide(leftNode);
            right = new PassThruChannelSide(rightNode);

            // Link left and right together
            left.paired = right;
            right.paired = left;
        }

        public void interruptFor(NodeAddress nodeAddress) {
            left.interruptFor(nodeAddress);
            right.interruptFor(nodeAddress);
        }

        public void restoreFor(NodeAddress nodeAddress) {
            left.restoreFor(nodeAddress);
            right.restoreFor(nodeAddress);
        }

        @Override
        public ChannelSide getLeft() {
            return left;
        }

        @Override
        public ChannelSide getRight() {
            return right;
        }
    }

    private class PassThruChannelSide implements ChannelSide {
        private final RaftNode raftNode;
        private PassThruChannelSide paired;
        private Consumer<Object> receiver;
        private volatile boolean interrupted = false;

        private PassThruChannelSide(RaftNode raftNode) {
            this.raftNode = raftNode;
        }

        // The node THIS side belongs to (the local endpoint). Distinct from the
        // ChannelSide#getAddress() contract, which reports the PAIRED (remote) node --
        // that contract is relied on by NodeConnectionOutboundImpl and must not change.
        private NodeAddress ownAddress() {
            return raftNode.getNodeAddress();
        }

        private void interruptFor(NodeAddress nodeAddress) {
            if (ownAddress().equals(nodeAddress)) {
                interrupted = true;
            }
        }

        private void restoreFor(NodeAddress nodeAddress) {
            if (ownAddress().equals(nodeAddress)) {
                interrupted = false;
            }
        }

        @Override
        public NodeAddress getAddress() {
            return paired.raftNode.getNodeAddress();
        }

        @Override
        public void setReceiver(Consumer<Object> receiver) {
            this.receiver = receiver;
        }

        // Baseline per-hop delay (ms): 1..9, identical to the pre-fault-injection behaviour.
        private long baselineDelayMillis() {
            return 1 + random.nextInt(9);
        }

        @Override
        public void send(Object message) {
            if (interrupted) {
                return;
            }
            final NodeFault fault = profile(ownAddress());
            if (fault.dropped(random)) {
                LOGGER.debug("PassThru send dropped {} -> {} by injected loss (rate {})",
                        ownAddress().getAddress(), getAddress().getAddress(), fault.packetLossRate);
                return;
            }
            schedule(baselineDelayMillis() + fault.latencyFloorMillis, () -> {
                if (!interrupted) {
                    paired.receive(message);
                }
            }, "Send");
        }

        private void receive(Object message) {
            if (interrupted) {
                return;
            }
            final NodeFault fault = profile(ownAddress());
            if (fault.dropped(random)) {
                LOGGER.debug("PassThru receive dropped {} -> {} by injected loss (rate {})",
                        getAddress().getAddress(), ownAddress().getAddress(), fault.packetLossRate);
                return;
            }
            schedule(baselineDelayMillis() + fault.latencyFloorMillis, () -> receiver.accept(message), "Receive");
        }

        private void schedule(long delayMillis, Runnable hop, String label) {
            final CompletableFuture<Void> future = CompletableFuture
                    .runAsync(hop, CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS))
                    .exceptionally(throwable -> {
                        LOGGER.error(label + " failed", throwable);
                        return null;
                    });
            addAndCleanFuture(future);
        }
    }

    /**
     * Immutable per-node fault profile. A "healthy" node (absent in the map) is equivalent to
     * {@link #NONE}: zero added latency and zero loss, so the baseline network (1..9 ms/hop)
     * is unchanged.
     */
    private static final class NodeFault {
        static final NodeFault NONE = new NodeFault(0L, 0.0);
        final long latencyFloorMillis;
        final double packetLossRate;

        NodeFault(long latencyFloorMillis, double packetLossRate) {
            this.latencyFloorMillis = latencyFloorMillis;
            this.packetLossRate = packetLossRate;
        }

        boolean dropped(Random random) {
            return random.nextDouble() < packetLossRate;
        }

        boolean isNone() {
            return latencyFloorMillis == 0L && packetLossRate == 0.0;
        }
    }
}
