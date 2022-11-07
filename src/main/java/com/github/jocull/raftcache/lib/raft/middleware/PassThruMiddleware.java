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

    public void interrupt(NodeAddress nodeAddress) {
        synchronized (connections) {
            connections.forEach(c -> {
                if (c.left.getAddress().equals(nodeAddress) || c.right.getAddress().equals(nodeAddress)) {
                    c.interrupt();
                }
            });
        }
    }

    public void restore(NodeAddress nodeAddress) {
        synchronized (connections) {
            connections.forEach(c -> {
                if (c.left.getAddress().equals(nodeAddress) || c.right.getAddress().equals(nodeAddress)) {
                    c.restore();
                }
            });
        }
    }

    @Override
    public ChannelMiddleware.ChannelSide openChannel(RaftNode source, NodeAddress targetAddress) {
        final RaftNode target = addressRaftNodeMap.get(targetAddress);
        if (target == null) {
            throw new IllegalStateException("Target node " + targetAddress.getAddress() + " was not registered!");
        }
        final PassThruPair pair = new PassThruPair(source, target);
        target.onConnection(pair.getRight());
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

        public void interrupt() {
            left.interrupted = true;
            right.interrupted = true;
        }

        public void restore() {
            left.interrupted = false;
            right.interrupted = false;
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
