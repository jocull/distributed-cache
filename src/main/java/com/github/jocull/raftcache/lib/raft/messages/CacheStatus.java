package com.github.jocull.raftcache.lib.raft.messages;

/**
 * The outcome of a cache verb (client B, B §3.1, 2026-11-25) — the Java mirror of the
 * proto {@code CacheStatus} enum in {@code wire.proto}. {@code OK} is the success case;
 * {@code NOT_LEADER} is the typed retryable error (B §3.4 first cut): the node is not
 * the leader, so the client re-discovers and retries. {@code UNSPECIFIED} mirrors the
 * proto zero value so every wire value round-trips 1:1; a well-formed response always
 * carries one of the two defined values.
 */
public enum CacheStatus {
    UNSPECIFIED,
    OK,
    NOT_LEADER
}
