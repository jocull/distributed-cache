package com.github.jocull.raftcache.proto.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);
    private static AtomicInteger nodeThreadCounter = new AtomicInteger(1);

    private final String nodeId;
    private final BlockingDeque<Objects> mailbox = new LinkedBlockingDeque<>();
    private final List<RaftNode> peers = new CopyOnWriteArrayList<>();

    private RaftNodeBehavior behavior = new RaftNodeBehavior();
    private Thread nodeThread;
    private volatile boolean stopped = false;

    public RaftNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized void start() {
        if (nodeThread != null) {
            throw new IllegalStateException("Already started");
        }

        stopped = false;
        nodeThread = new Thread(new NodeThread());
        nodeThread.setName("node-thread-" + nodeThreadCounter.getAndIncrement());
        nodeThread.start();
    }

    public synchronized void stop() {
        if (nodeThread == null) {
            throw new IllegalStateException("Not started");
        }

        stopped = true;
        nodeThread.interrupt();
    }

    public void addPeer(RaftNode node) {
        if (peers.contains(node)) {
            throw new IllegalStateException("Peer already exists");
        }
        peers.add(node);
    }

    public void removePeer(RaftNode node) {
        if (!peers.remove(node)) {
            throw new IllegalStateException("Peer does not exist");
        }
    }

    private class NodeThread implements Runnable {
        @Override
        public void run() {
            while (!stopped) {
                try {
                    final Object message = mailbox.poll(10, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        behavior.onMessage(message);
                    }
                    behavior.heartbeat();
                } catch (Exception ex) {
                    LOGGER.error("Dispatcher error", ex);
                }
            }
        }

        private void stop() {
            stopped = true;
            nodeThread.interrupt();
        }
    }

    @Override
    public String toString() {
        return "RaftNode{" +
                "nodeId='" + nodeId + '\'' +
                ", nodeThread=" + nodeThread +
                '}';
    }
}
