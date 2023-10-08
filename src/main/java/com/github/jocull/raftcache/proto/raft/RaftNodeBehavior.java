package com.github.jocull.raftcache.proto.raft;

/**
 * What should the raft node do?
 * - Needs to send messages
 *      - Introducing self as a new node
 *      - Shipping logs
 *      - Acknowledging logs
 *      - Asking for votes
 *      - Sending out a vote
 * - Needs to understand messages
 *      - Heartbeats, with any new logs
 *      - Vote requests, for a new term
 * - Behavior dictates the messages sent, understood, and dropped
 */
class RaftNodeBehavior {
    /**
     * In the initial state, new nodes only want to find, and follow, the leader of the current term
     */
    private class InitialFollower {
    }

    /**
     * Followers only want to follow the leader of the current term, unless it gets too quiet...
     */
    private class Follower {
    }

    /**
     * Candidates only want to try to become the leader of a new term... or try again
     */
    private class Candidate {
    }

    /**
     * Leaders ship logs to followers, unless they see they've been replaced in a new term
     */
    private class Leader {
    }
}
