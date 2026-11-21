package com.github.jocull.raftcache.lib.raft.middleware;

import java.io.Serializable;

/**
 * The reply frame a {@link SocketChannelMiddleware} server writes once its node has
 * accepted the connection and its receiver sink is installed. The client's
 * {@code openChannel} blocks until this frame is read (see {@code NETWORKING.md} 2.2).
 */
public class Acknowledgement implements Serializable {
    private static final long serialVersionUID = 1L;
}
