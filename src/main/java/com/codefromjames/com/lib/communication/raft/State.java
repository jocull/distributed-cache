package com.codefromjames.com.lib.communication.raft;

import java.util.List;

public class State {
    // Persistent state on all servers:
    long currentTerm; // last term server has seen (starts at 0 and increments)
    String votedFor; // candidateId that received vote in current term (or null if none)
    List<Object> logs; // log entries, one per command for state machine

    // Volatile state; All servers
    long commitIndex; // index of highest log entry known to be committed (starts at 0 and increments)
    long lastApplied; // index of highest log entry applied to state machine (starts at 0 and increments)

    // Volatile state; Leaders only
    // keep track of these for all followers, reset after each election
    List<Long> nextIndex; // next index, pointer to last received (starts at last leader log + 1)
    List<Long> matchIndex; // highest log entry known to be replicated (starts at zero)
}
