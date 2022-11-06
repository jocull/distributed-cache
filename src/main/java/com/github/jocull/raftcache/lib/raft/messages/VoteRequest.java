package com.github.jocull.raftcache.lib.raft.messages;

public class VoteRequest {
    private final int term;
    private final String candidateId;
    private final TermIndex lastLogTermIndex;

    public VoteRequest(int term, String candidateId, TermIndex lastLogTermIndex) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogTermIndex = lastLogTermIndex;
    }

    public int getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public TermIndex getLastLogTermIndex() {
        return lastLogTermIndex;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", lastLogTermIndex=" + lastLogTermIndex +
                '}';
    }
}
