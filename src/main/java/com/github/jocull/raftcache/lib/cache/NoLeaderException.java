package com.github.jocull.raftcache.lib.cache;

/**
 * The cluster has no leader available to the client right now: discovery probed the bootstrap
 * nodes (and the cluster views they reported) and no live node answered as LEADER.
 *
 * <p>RETRYABLE, by design (client B §4.2, 2026-11-25): an election may be in progress, the
 * current leader may be behind a partition, or a reported leader may have stepped down. The
 * caller may simply retry the same operation — the client re-discovers on each attempt — this
 * is the client-side counterpart of the in-process facade's {@link IllegalStateException}
 * refusal when {@code RaftNode#submit} has no leader to route to.
 */
public class NoLeaderException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public NoLeaderException(String detail) {
        super(detail);
    }
}
