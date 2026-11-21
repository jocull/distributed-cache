package com.github.jocull.raftcache.lib.raft.messages;

/**
 * A Pre-Vote response: the voter reports whether it would grant a vote to the candidate for
 * {@code term} if the candidate started a real election now. A grant is a pure prediction —
 * it has already cost the voter nothing and does not advance any term.
 *
 * <p>Per pre-vote semantics a response never advances the receiver's term, even when
 * {@code term} is greater than the receiver's current term.
 */
public class PreVoteResponse {
    private final int term;
    private final boolean preVoteGranted;

    public PreVoteResponse(int term, boolean preVoteGranted) {
        this.term = term;
        this.preVoteGranted = preVoteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isPreVoteGranted() {
        return preVoteGranted;
    }

    @Override
    public String toString() {
        return "PreVoteResponse{" +
                "term=" + term +
                ", preVoteGranted=" + preVoteGranted +
                '}';
    }
}
