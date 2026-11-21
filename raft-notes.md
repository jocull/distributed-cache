## Helpful links and documents:

- Official website, links, project repos, talks, more https://raft.github.io/
- Official paper: https://raft.github.io/raft.pdf
    - Page 4 includes the best summary, with some example data models and rules
- Apache Ratis, one Java implementation with some examples https://ratis.apache.org/
- SOFAJRaft, another Java implementation with some examples https://github.com/sofastack/sofa-jraft
- gRPC wiki, which some of the above implementations chose https://en.wikipedia.org/wiki/GRPC
    - In Java: https://github.com/grpc/grpc-java
- JSON-RPC wiki, which might be more compatible due to less complex implementation https://en.wikipedia.org/wiki/JSON-RPC
    - Spring tutorial on implementing WebSockets in Java https://spring.io/guides/gs/messaging-stomp-websocket/

## Notes from "Secret Lives of Data" demonstration

As linked from official website: http://thesecretlivesofdata.com/raft/

This helpful guide demonstrates lifecycles and phases in Raft so they are easy to see and understand.

### Leader elections

#### Election timeout
  - Amount of time a follower waits until becoming a candidate
  - Randomized to be between 150ms and 300ms
  - If a node hits election timeout a new election term starts

#### Election term

- Node hitting timeout votes for itself
    - Sends `RequestVote` message out to other nodes
- If node receiving hasn't voted yet in this term then it votes for the candidate and waits for next election timeout
- Candidate with majority of votes becomes leader
- Leader begins sending out `AppendEntries` messages to followers (heartbeats? logs?)
    - Messages are sent in intervals specified by the heartbeat timeout
    - Followers respond to append entries with an ack basically
    - On the next heartbeat, the followers are informed that the entry was committed, making it final on their end
- Election term will continue until a follower stops receiving heartbeats and becomes a candidate
    - If any leader node sees a higher term number appear it will step down

  ### Log replication

- Leaders need to replicate all changes to followers
    - Uses same append entries as for heartbeats
- Client sends a change to leader
    - Change is appended to leader's log
    - On next heartbeat, change is sent to followers
    - Entry is committed once a majority of followers acknowledge it
        - Which triggers a response to the client
        - If a majority cannot acknowledge, it doesn't move forward
- Any uncommitted logs are rolled back (discarded) if a node sees an election term change

## Spec verification: slow-follower / rejoin liveness (2026-11-20)

Context: `RaftNodeFlakySlowNodeTest.java` — in a 3-node cluster where one node (C) has
~200 ms+ round-trip latency to the others, the cluster livelocks: terms spin (observed
1 to 69 in one run), C's log never advances, and `testSlowFollower_catchesUp_afterLeaderCommits`
can never see index 8 on C before its timeout. Before changing any code we verified
exactly what the spec requires for this case. Quotes below were extracted verbatim from the
PDFs on 2026-11-20 (minor text-extraction artifacts normalized, e.g. `Pre-V ote` →
`Pre-Vote`, dropped punctuation, `Cnew` → `C_new`).

**Verdict: the implementation follows the basic Raft algorithm correctly, and the failing
liveness case is a documented limitation of the *basic* algorithm. The standard optional
fix is the Pre-Vote extension (Ongaro thesis §9.6), which production etcd uses and made
default-on in v3.5.** The paper itself already admits the disruption problem (§6) for the
membership-change case.

### 1. Bumping the term at candidacy is MANDATORY (paper §5.2)

> To begin an election, a follower increments its current term and transitions to candidate
> state. It then votes for itself and issues RequestVote RPCs in parallel to each of the
> other servers in the cluster.

> Each server will vote for at most one candidate in a given term, on a first-come-first-served
> basis (note: Section 5.4 adds an additional restriction on votes).

Source: Diego Ongaro & John Ousterhout, "In Search of an Understandable Consensus Algorithm
(Raft)", USENIX ATC 2014, §5.2 — https://raft.github.io/raft.pdf

Our code: `RaftNodeBehaviorCandidate` does exactly this (term bumped when candidacy begins).

### 2. The up-to-date vote denial is MANDATORY (paper §5.4.1 "Election restriction")

> In any leader-based consensus algorithm, the leader must eventually store all of the
> committed log entries. ... Raft uses a simpler approach where it guarantees that all the
> committed entries from previous terms are present on each new leader from the moment of its
> election, without the need to transfer those entries to the leader.

> Raft uses the voting process to prevent a candidate from winning an election unless its log
> contains all committed entries. A candidate must contact a majority of the cluster in order
> to be elected, which means that every committed entry must be present in at least one of
> those servers. If the candidate's log is at least as up-to-date as any other log in that
> majority (where "up-to-date" is defined precisely below), then it will hold all the
> committed entries. The RequestVote RPC implements this restriction: the RPC includes
> information about the candidate's log, and the voter denies its vote if its own log is more
> up-to-date than that of the candidate.

> Raft determines which of two logs is more up-to-date by comparing the index and term of the
> last entries in the logs. If the logs have last entries with different terms, then the log
> with the later term is more up-to-date. If the logs end with the same term, then whichever
> log is longer is more up-to-date.

Source: same paper, §5.4.1 — https://raft.github.io/raft.pdf

Our code: `RaftNodeBehaviorFollower` denies the vote unless the candidate's
(last term, last index) is ≥ its own — exactly this rule. In the failing test A and B have
committed entries and C's log is (0,0), so A/B denying C is *required by the paper*.

### 3. The paper itself already concedes the disruption problem (paper §6, reconfiguration)

> The third issue is that removed servers (those not in C_new) can disrupt the cluster. These
> servers will not receive heartbeats, so they will time out and start new elections. They
> will then send RequestVote RPCs with new term numbers, and this will cause the current
> leader to revert to follower state. A new leader will eventually be elected, but the removed
> servers will time out again and the process will repeat, resulting in poor availability.
> To prevent this problem, servers disregard RequestVote RPCs when they believe a current
> leader exists. Specifically, if a server receives a RequestVote RPC within the minimum
> election timeout of hearing from a current leader, it does not update its term or grant its
> vote.

Source: same paper, §6 — https://raft.github.io/raft.pdf

Note: this is a **voter-side** guard scoped to the membership-change discussion. The general
isolated-then-rejoined case is treated in the thesis below. A text search of the paper
confirms it contains **no** "Pre-Vote", no "livelock", and no discussion of a rejoining
server (its only "liveness" mention is in the Paxos introduction).

### 4. Thesis §9.6 — the exact documented case, and the standard fix (Ongaro thesis, verbatim)

> **9.6 Preventing disruptions when a server rejoins the cluster**
>
> One downside of Raft's leader election algorithm is that a server that has been partitioned
> from the cluster is likely to cause a disruption when it regains connectivity. When a server
> is partitioned, it will not receive heartbeats. It will soon increment its term to start an
> election, although it won't be able to collect enough votes to become leader. When the server
> regains connectivity sometime later, its larger term number will propagate to the rest of the
> cluster (either through the server's RequestVote requests or through its AppendEntries
> response). This will force the cluster leader to step down, and a new election will have to
> take place to select a new leader. Fortunately, such events are likely to be rare, and each
> will only cause one leader to step down.
>
> If desired, Raft's basic leader election algorithm can be extended with an additional phase
> to prevent such disruptions, forming the **Pre-Vote** algorithm. In the Pre-Vote algorithm, a
> candidate only increments its term if it first learns from a majority of the cluster that they
> would be willing to grant the candidate their votes (if the candidate's log is sufficiently
> up-to-date, and the voters have not received heartbeats from a valid leader for at least a
> baseline election timeout). This was inspired by ZooKeeper's algorithm [42], in which a server
> must receive a majority of votes before it calculates a new epoch and sends NewEpoch messages
> (however, in ZooKeeper servers do not solicit votes, other servers offer them).
>
> The Pre-Vote algorithm solves the issue of a partitioned server disrupting the cluster when
> it rejoins. While a server is partitioned, it won't be able to increment its term, since it
> can't receive permission from a majority of the cluster. Then, when it rejoins the cluster, it
> still won't be able to increment its term, since the other servers will have been receiving
> regular heartbeats from the leader. Once the server receives a heartbeat from the leader
> itself, it will return to the follower state (in the same term).
>
> We recommend the Pre-Vote extension in deployments that would benefit from additional
> robustness. We also tested it in various leader election scenarios in AvailSim, and it does
> not appear to significantly harm election performance.

Source: Diego Ongaro, "Consensus: Hoarded Gossip and Fast, Log-Structured Replication", PhD
thesis, Stanford, 2014, Chapter 9 §9.6 (pp. 136–137) —
https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf

### 5. How a production implementation handles it — etcd (live-fetched 2026-11-20)

- `raft/raft.go` (identical in v3.4.14 and v3.5.15):

  ```go
  // PreVote enables the Pre-Vote algorithm described in raft thesis section
  // 9.6. This prevents disruption when a node that has been partitioned away
  // rejoins the cluster.
  PreVote bool
  ```

  → https://github.com/etcd-io/etcd/blob/v3.5.15/raft/raft.go

- `server/embed/config.go` (v3.5.15):

  ```go
  // PreVote is true to enable Raft Pre-Vote.
  // If enabled, Raft runs an additional election phase
  // to check whether it would get enough votes to win
  // an election, thus minimizing disruptions.
  PreVote bool `json:"pre-vote"`
  ```

  → https://github.com/etcd-io/etcd/blob/v3.5.15/server/embed/config.go

- v3.5.15 defaults the flag on (`PreVote: true` in `NewConfig`); v3.4 defaults it off, so
  default-on from the v3.4 → v3.5 line.
- Op documentation: `--pre-vote 'true'` — "Enable the raft Pre-Vote algorithm to prevent
  disruption when a node that has been partitioned away rejoins the cluster." →
  https://etcd.io/docs/v3.6/op-guide/configuration

### 6. What this means for our slow-follower livelock (3-node, C slow ≥ 200 ms/hop)

1. From A/B's point of view C is effectively the thesis's "partitioned server": its
   election timeout (150–300 ms) always fires before any append can reach it (an
   A/B→C hop alone is ≥ 200 ms), so its candidacy bumps the term, its RequestVote
   propagates the higher term and forces the current leader to step down, and A/B
   re-elect each other (term keeps climbing). All 79 vote lines in the failing run are
   `grant=false` to C — consistent with the §5.4.1 rule both A and B must apply.
2. Every append C does receive carries a term ≤ C's (C NACKs / stays candidate), or
   arrives after C has already bumped its term again. C's log never leaves (0,0) and
   therefore can never be up-to-date: a chicken-and-egg livelock that lasts as long as
   the latency does.
3. This is **not** a spec deviation: the basic algorithm is faithfully implemented, and
   the liveness gap is exactly the one the thesis documents and the paper foreshadows
   (§6). Pre-Vote — the optional extension the thesis prescribes and etcd has
   default-enabled since v3.5 — removes the loop by not letting C increment its term
   while a majority would not (hypothetically) vote for it.
4. Remaining choice: (a) implement Pre-Vote in `src/main` (protocol change — needs
   sign-off), (b) re-scope the affected test to what the basic algorithm guarantees
   (A+B must still be able to commit while C is slow) and document the limitation, or
   (c) both.
