package com.github.jocull.raftcache.proto.raft;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    // Manual playground for the RaftNode sketch: the whole body is a Thread.sleep(60_000) with
    // no assertions, which would cost every mvn build ~57 s of dead time. Remove the @Disabled
    // (without committing) to run it by hand for debugging.
    @Disabled("manual debugging playground -- 60 s sleep, no assertions")
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