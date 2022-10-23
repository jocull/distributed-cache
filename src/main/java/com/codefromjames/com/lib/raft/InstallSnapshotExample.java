package com.codefromjames.com.lib.raft;

public class InstallSnapshotExample {
    // arguments
    long term; // leader's term
    String leaderId; // so follower can redirect clients
    long lastIncludedIndex; // the snapshot replaces all entries up through and including this index
    long lastIncludedTerm; // term of lastIncludedIndex
    long offset; // byte offset where chunk is positioned in the snapshot file (for splitting sends?)
    byte[] data; // raw bytes of the snapshot chunk, starting at the offset
    boolean done; // true if this is the last chunk

    // results (probably another class)
    long rTerm; // current term, for the leader to update itself
}
