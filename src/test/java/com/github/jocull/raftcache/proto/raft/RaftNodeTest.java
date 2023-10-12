package com.github.jocull.raftcache.proto.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    @Test
    void basics() throws InterruptedException {
        RaftNode a = new RaftNode("a");
        RaftNode b = new RaftNode("b");
        RaftNode c = new RaftNode("c");
        List<RaftNode> nodes = List.of(a, b, c);
        for (RaftNode self : nodes) {
            for (RaftNode other : nodes) {
                if (self != other) {
                    self.addPeer(other);
                }
            }
        }
        for (RaftNode self : nodes) {
            self.start();
        }

        Thread.sleep(60_000);
    }
}