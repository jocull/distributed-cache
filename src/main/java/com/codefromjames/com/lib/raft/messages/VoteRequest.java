package com.codefromjames.com.lib.raft.messages;

public class VoteRequest {
    private final int term;
    private final String candidateId;
    private final long lastLogIndex;
    private final int lastLogTerm;

    public VoteRequest(int term, String candidateId, long lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
