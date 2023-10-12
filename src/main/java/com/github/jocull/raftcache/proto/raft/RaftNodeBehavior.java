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
 *      - The cluster topology, current term, and index
 *      - Heartbeats, with any new logs
 *      - Vote requests, for a new term
 * - Behavior dictates the messages sent, understood, and dropped
 */
class RaftNodeBehavior {
    private Behavior behavior = new InitialFollower();

    public void heartbeat() {
        behavior.heartbeat();
    }

    public void onMessage(Object message) {
        behavior.onMessage(message);
    }

    private interface Behavior {
        // TODO: How do behaviors delegate to send and receive messages?
        void heartbeat();

        void onMessage(Object message);
    }

    /**
     * In the initial state, new nodes only want to find, and follow, the leader of the current term
     */
    private class InitialFollower implements Behavior {
        @Override
        public void heartbeat() {

        }

        @Override
        public void onMessage(Object message) {

        }
    }

    /**
     * Followers only want to follow the leader of the current term, unless it gets too quiet...
     */
    private class Follower implements Behavior {
        @Override
        public void heartbeat() {
            // We should see a heartbeat from the leader within the timeout
        }

        @Override
        public void onMessage(Object message) {

        }
    }

    /**
     * Candidates only want to try to become the leader of a new term... or try again
     */
    private class Candidate implements Behavior {
        @Override
        public void heartbeat() {
            // We're voting for ourselves. We're only waiting for votes to come in before the timeout.
        }

        @Override
        public void onMessage(Object message) {

        }
    }

    /**
     * Leaders ship logs to followers, unless they see they've been replaced in a new term
     */
    private class Leader implements Behavior {
        @Override
        public void heartbeat() {
            // We need to send out regular heartbeats to avoid triggering the heartbeat timeout on followers.
        }

        @Override
        public void onMessage(Object message) {

        }
    }
}
