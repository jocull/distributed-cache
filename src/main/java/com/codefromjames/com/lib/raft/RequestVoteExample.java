package com.codefromjames.com.lib.raft;

public class RequestVoteExample {
    // arguments
    long term; // candidate's term
    String candidateId; // candidate requesting vote
    long lastLogIndex; // index of candidate's last log entry
    long lastLogTerm; // term of candidate's last log entry

    // results (probably another class)
    long rTerm; // current term, for the candidate update itself
    boolean voteGranted; // true means candidate received vote
}
