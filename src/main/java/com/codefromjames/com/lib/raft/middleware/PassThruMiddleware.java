package com.codefromjames.com.lib.raft.middleware;

import com.codefromjames.com.lib.raft.RaftNode;
import com.codefromjames.com.lib.topology.NodeAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A pass-thru object middleware, great for testing "networks" in memory to validate
 * communication strategies between objects.
 */
public class PassThruMiddleware implements ChannelMiddleware {
    private final Map<NodeAddress, RaftNode> addressRaftNodeMap = new HashMap<>();

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

    @Override
    public ChannelMiddleware.ChannelSide openChannel(RaftNode source, NodeAddress targetAddress) {
        final RaftNode target = addressRaftNodeMap.get(targetAddress);
        if (target == null) {
            throw new IllegalStateException("Target node " + targetAddress.getAddress() + " was not registered!");
        }
        final PassThruPair pair = new PassThruPair(source, target);
        target.onConnection(pair.getRight());
        return pair.getLeft();
    }

    private static class PassThruPair implements ChannelMiddleware.ChannelPair {
        private final PassThruChannelSide left;
        private final PassThruChannelSide right;

        public PassThruPair(RaftNode leftNode, RaftNode rightNode) {
            left = new PassThruChannelSide(leftNode);
            right = new PassThruChannelSide(rightNode);

            // Link left and right together
            left.paired = right;
            right.paired = left;
        }

        @Override
        public ChannelSide getLeft() {
            return left;
        }

        @Override
        public ChannelSide getRight() {
            return right;
        }

        private static class PassThruChannelSide implements ChannelSide {
            private final RaftNode raftNode;
            private PassThruChannelSide paired;
            private Consumer<Object> receiver;

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
                paired.receive(message);
            }

            private void receive(Object message) {
                receiver.accept(message);
            }
        }
    }
}
