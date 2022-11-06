package com.github.jocull.raftcache.lib.raft.messages;

public class VoteResponse {
    private final int term;
    private final TermIndex committedTermIndex;
    private final boolean voteGranted;

    public VoteResponse(int term, TermIndex committedTermIndex, boolean voteGranted) {
        this.term = term;
        this.committedTermIndex = committedTermIndex;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public TermIndex getCommittedTermIndex() {
        return committedTermIndex;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "VoteResponse{" +
                "term=" + term +
                ", committedTermIndex=" + committedTermIndex +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
