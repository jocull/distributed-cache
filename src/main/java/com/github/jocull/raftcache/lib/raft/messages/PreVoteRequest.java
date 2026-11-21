package com.github.jocull.raftcache.lib.raft.messages;

/**
 * A Pre-Vote request (etcd's "pre-vote", Raft §5.2.4-inspired).
 *
 * <p>The candidate asks whether it <em>would</em> receive a vote if it started a real election
 * in {@code term}, WITHOUT the receiving node changing any of its state: no term advancement,
 * no {@code votedFor} mutation, no timer reset. The only side effect permitted on the voter
 * is sending back a {@link PreVoteResponse}.
 */
public class PreVoteRequest {
    private final int term;
    private final String candidateId;
    private final TermIndex lastLogTermIndex;

    public PreVoteRequest(int term, String candidateId, TermIndex lastLogTermIndex) {
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
        return "PreVoteRequest{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", lastLogTermIndex=" + lastLogTermIndex +
                '}';
    }
}
