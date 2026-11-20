package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
        Objects.requireNonNull(raftNode);
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

        @Override
        public void send(Object message) {
            if (!interrupted) {
                final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            if (!interrupted) {
                                paired.receive(message);
                            }
                        }, CompletableFuture.delayedExecutor(1 + random.nextInt(9), TimeUnit.MILLISECONDS))
                        .exceptionally(throwable -> {
                            LOGGER.error("Send failed", throwable);
                            return null;
                        });
                addAndCleanFuture(future);
            }
        }

        private void receive(Object message) {
            if (!interrupted) {
                final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            if (!interrupted) {
                                receiver.accept(message);
                            }
                        }, CompletableFuture.delayedExecutor(1 + random.nextInt(9), TimeUnit.MILLISECONDS))
                        .exceptionally(throwable -> {
                            LOGGER.error("Receive failed", throwable);
                            return null;
                        });
                addAndCleanFuture(future);
            }
        }
    }
}
