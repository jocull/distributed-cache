# Snapshots & Log Compaction — Investigation and Staged Plan (work-plan item 4)

**Status: DONE — investigation + design delivered 2026-11-21, then all implementation stages
delivered 2026-11-23.** This document began as the sign-off-approved *"look into doing
snapshots/log compaction"* — the in-repo investigation plus staged implementation plan — and §4's
plan was subsequently executed in full, each stage committed with a green `mvn -o -B test`:

| Stage (per §4) | Delivered | Commit |
|---|---|---|
| 1 — state capture & wholesale replacement | `Snapshot`, capture/restore, bridge adopt | `47d9f9f` |
| 2 — log boundary and compaction in `RaftLogs` (S2/S3/S5) | snapshot boundary + `compact()` | `a96abc8` |
| 3 — `InstallSnapshot` protocol messages and wire plumbing | request/response + wire | `e9102f1` |
| 4 — follower `InstallSnapshot` acceptance and bridge adoption | install handler behind S2 | `d658475` |
| 5 — leader laggard `InstallSnapshot` + compaction trigger (+ per-node snapshot supplier wiring addendum) | `SnapshotTransfer` + save-before-trim gate | `3fd7e6c`, `9f0a705` |
| 6(a) | chunked transfer: leader pins one serialization, follower assembles, offset cursor resumes at the returned offset | `35f4ec0` |
| 6(b) | durable `SnapshotStore` (in-memory + file impls): save-on-compact (leader), save-on-install (follower — a failed save aborts before the trim), seed at startup before log replay; null/empty/corrupt/throwing stores warn and skip, startup is never blocked | `stage 6(b)` |

Final suite at stage 6(b): **320 tests green, 0 failures, 1 pre-existing skip**
(`/tmp/opencode/mvn-phase4-final.log`). `InstallSnapshotExample.java` and the `proto/raft/*`
package were consulted as working notes only and remain untouched (do-not-modify).

Sources: Raft paper §7 (§7.1 log compaction, §7.2 snapshot transfer) and §5.4.1 (up-to-date rule,
which snapshots interact with), Ongaro's thesis (log compaction + snapshot chapters), etcd's
production `InstallSnapshot`/compaction practice. All code citations below are line-verified against
the `finish` branch at the time of writing.

---

## 1. Ground truth in this repo

### 1.1 The log — `lib/raft/RaftLogs.java`

- The log is an `ArrayDeque<CompletableRaftLog<?>>` seeded at construction with one fixed
  start point: `EPOCH_LOG` = `TermIndex.EPOCH` (term 0, index 0)
  (`RaftLogs.java:16-28`; comment: *"Adding the epoch log as a start point simplifies operations
  checking indexes against a start point"*).
- `commit(TermIndex)` advances `committedTermIndex` and publishes the newly committed entries to the
  event bus (this is what `StateMachineBridge` consumes).
- `rollback()` (`RaftLogs.java:124`) trims the **uncommitted tail only**. There is **no operation
  anywhere that removes committed entries** — the log grows without bound for the lifetime of the
  node. This is the entire memory-footprint problem snapshots exist to solve.
- `containsStartPoint(TermIndex)` (`RaftLogs.java:39-46`) is a linear
  `logs.stream().anyMatch(...)` equality scan, with the author's own two TODOs:
  *"Could probably be optimized by using actual offsets… track the 'offset of offsets' to keep things
  consistent **even after a snapshot**"* and *"It would probably be faster to walk this backwards"*.
  The snapshot design below supersedes both TODOs.
- `getLogRange(start, limit)` also walks the deque (performance TODO at `RaftLogs.java:68`).
- Term/index bookkeeping (`currentTermIndex`, `committedTermIndex`) is field-based, independent of
  deque contents — convenient for compaction: those readers keep working after entries are removed.

### 1.2 Messages — `lib/raft/messages/`

`AppendEntries` (term, `previousLogTermIndex`, `leaderCommitTermIndex`, entries;
`AppendEntries.java:7-15`) is answered by `AcknowledgeEntries`. There is **no `InstallSnapshot`
request or response** and no other snapshot-shaped message in the package (full list: `AcknowledgeEntries`,
`AppendEntries`, `AnnounceClusterTopology`, `Introduction`, `NodeIdentifier`, `PreVoteRequest`,
`PreVoteResponse`, `Request`, `Response`, `StateRequest`, `StateResponse`, `TermIndex`,
`VoteRequest`, `VoteResponse`).

### 1.3 Behaviors

- Follower (`lib/raft/RaftNodeBehaviorFollower.java`): `onAppendEntries` does its log-match
  catch-up through `containsStartPoint(prevLogIndex)` — if the follower's log no longer contains the
  leader's previous index it cannot accept. Today that path has no escape hatch: a log that has been
  compacted *must* be able to answer "I need a snapshot" instead (see Stage 4). There is no snapshot
  accept handler and no role change other than the existing leader/term step-downs.
- Leader (`lib/raft/RaftNodeBehaviorLeader.java`): per-follower `AppendEntries`/heartbeat loop and
  majority commit math in `updateCommittedIndex` (~`:174-186`). No "far-behind follower" detection,
  no snapshot sending, no compaction trigger.
- State machine (`lib/data/PartitionMap.java`): `getSnapshot()` **already exists**
  (`PartitionMap.java:121-126`) — it returns `Map<Partition, Map<String, KeyDataOutput>>` by copying
  each partition's view, with a TODO about the map's global version at snapshot time. **Nothing calls
  it.** `sweepExpired()` / per-entry `DataVersion` (value + version counter + absolute
  `expiresAtEpochMillis`, `NO_EXPIRY = Long.MAX_VALUE`) is a deterministic function of the log — the
  same state every node computes after applying the same committed prefix.
- Bridge (`lib/data/StateMachineBridge.java`): applies committed logs in order, tracking the highest
  applied index and skipping ≤. It has **no "replace my state wholesale" path** — a follower that
  must adopt a snapshot (Stage 4) needs exactly that.

### 1.4 Working notes (do-not-modify), referenced but not built upon

- `lib/raft/InstallSnapshotExample.java`: sketch fields
  `term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done`. Note the sketch already
  anticipates **chunking** (`offset` + `done`) and carries the *last entry's* index/term inside the
  payload — matching paper §7.2 and etcd's `InstallSnapshot` metadata.
- `proto/raft/*` (main stubs, incl. empty `class RaftLogs {}`) and its `@Disabled`
  `proto.raft.RaftNodeTest`: unrelated empty stubs; the real log is `lib/raft/RaftLogs.java`. Not
  build targets of any feature here and not deleted (Decision #2).

## 2. Gap analysis vs. Raft §7

| Raft §7 requirement | State in this repo |
|---|---|
| **§7.1** — a leader may compact its log by taking a snapshot of the state machine and discarding entries up to the snapshot's tail, *provided no live follower still needs those entries to reconstruct its log* (the leader must keep whatever followers may need, and may drop its own copy of a snapshot once all followers have caught up) | **Nothing.** Committed entries are never removed; only the uncommitted tail is ever trimmed (`rollback()`) |
| **§7.2** — a leader periodically sends its snapshot to a follower in place of `AppendEntries`: the last included index and term plus the snapshot data; the follower installs it and truncates its log | **Nothing.** No message, no handler, no leader-side trigger |
| **§5.4.1 interplay** — after compaction, a node's "last entry" for the up-to-date rule is the snapshot's last included index/term | Would require the log to expose its post-compaction boundary (§3 S3); currently `RaftLogs` would report past the removed entries, corrupting elections |
| Election-safety of a *compacted follower* (must not vote itself up on stale state) | Same source as above |

Practical consequence today: a long-lived cluster's committed entries (every PUT, DELETE, EXPIRE
ever sent) remain in every node's deque forever. DELETE/EXPIRE made the *map* forget; the *log* does
not. Snapshots are the only mechanism that closes that half.

## 3. Invariants to preserve (safety)

- **S1 — compaction bound.** Compact only up to an index no live follower has yet passed: the
  boundary must be ≤ the minimum `nextIndex` of live followers (a follower whose state was installed
  from an older snapshot counts as "past" it). This is the paper's exact rule and the etcd rule
  (compaction point = committed minus a threshold, floored by the slowest live follower).
- **S2 — progress-only install.** A follower accepts a snapshot only if it makes progress:
  `lastIncludedIndex > local log tail index` (i.e. never replaces newer/committed state with older —
  the etcd acceptance rule). This also makes install idempotent under Raft re-delivery/replay.
- **S3 — election up-to-date rule stays correct.** With an empty-ish compacted log, candidate/voter
  comparisons must fall back to the snapshot boundary's term/index (the node's "last entry" becomes
  the snapshot's last included index/term).
- **S4 — determinism.** Snapshot state must be a pure function of the committed log prefix: capture
  absolute values (data, per-key version counter, absolute `expiresAt` deadline) — never per-node
  clock state (the TTL design already stores absolute deadlines, which makes this hold).
- **S5 — monotonicity.** Compaction only ever moves the start point forward; `rollback()` may never
  remove past it.

## 4. Staged implementation plan (ordered by risk)

Stages 1–2 touch no protocol and are independently testable with the existing deterministic patterns
(`MutableClock`, `mock(EventBus.class)`, `RaftLogsTest`). Stages 3–5 are the protocol proper and
touch the timing-sensitive paths the 242-test suite guards (election windows 150–300 ms,
at-most-one-leader-per-term, apply-then-acknowledge).

**Stage 1 — state capture & wholesale replacement (data layer).**
New `Snapshot(term, index, Map<Partition, Map<String, KeyDataOutput>>)` built from the existing
`PartitionMap.getSnapshot()`; `PartitionMap.replaceState(snap)` restores it exactly (carrying the
`DataVersion` *version counter* through, so the next applied PUT for a key increments from the
captured version — resolves the `getSnapshot()` global-version TODO); `StateMachineBridge.adopt(snap)`
swaps state and re-anchors its last-applied index. *Tests:* capture → mutate → replace round-trip,
replace with a subset/ superset of keys, TTL deadlines survive, post-replace version increments are
correct, re-replace idempotency.

**Stage 2 — log boundary + compaction in `RaftLogs`.**
`RaftLogs` gains a nullable `snapshot` boundary (last included `TermIndex`) + payload holder:
`compact(TermIndex boundary, payload)`, `getStartTermIndex()` (boundary, else `TermIndex.EPOCH`),
`hasStartPointSnapshot()`. `compact()` drops deque entries ≤ boundary, replaces the seed point with
the boundary record (exactly like `EPOCH_LOG`, but real term/index), and leaves
`current/committedTermIndex` untouched (already field-based). `rollback()` stops at the boundary
(S5). `containsStartPoint(boundary)` is an O(1) field check — closing both `RaftLogs.java:39-44`
TODOs. `getLogRange` into a compacted region must report the gap (caller converts that into "send
snapshot" — Stage 4). *Tests:* existing `RaftLogsTest` + compaction-keeps-committed-index,
rollback-stops-at-boundary, `containsStartPoint` boundary hit, post-compaction term/index reads.
*Note:* S3 lands here — `getCurrentTermIndex()` already reads fields, so the "last entry" the
candidate reports must be re-sourced to the boundary when the deque tail lies before it.

**Stage 3 — protocol messages (`lib/raft/messages/`).**
`InstallSnapshot`: `term, leaderId, lastIncludedIndex, lastIncludedTerm, payload, done` — the
`InstallSnapshotExample` sketch shape verbatim (single chunk initially; `done`/`offset` are already
in the shape for chunking later). `InstallSnapshotResponse`: `index, term`. Dispatched through the
same request/response plumbing as `AppendEntries`.

**Stage 4 — follower install (`RaftNodeBehaviorFollower`).**
On `InstallSnapshot`: apply the existing term rules (higher term ⇒ step down, per the
`onAppendEntries` handling); guard with S2; on accept: `bridge.adopt(snap)` + `logs.compact(boundary, payload)` +
reply with the new log tail. The `onAppendEntries` catch-up path gains the escape hatch: when the
previous index lies in a compacted region, reply "snapshot needed" instead of failing the match
indefinitely — the leader's Stage-5 trigger.

**Stage 5 — leader send + compaction trigger (`RaftNodeBehaviorLeader`).**
Track per-follower laggard detection (nextIndex stuck far below `committedTermIndex`); switch that
follower to `InstallSnapshot` until acked, then resume `AppendEntries` (whose prev-index now lands
on the compacted follower's new start point — the Stage-2 fast path). Compaction trigger: when
`committed − min(live-follower nextIndex)` exceeds a threshold N and the state machine has applied
through the boundary (guaranteed by this repo's apply-then-acknowledge,
`RaftNodeBehaviorLeader` ~`:184-190`), `logs.compact(boundary, state)` per S1. *Tests:* a
`interrupt`/`restore`-held follower falls behind, cluster keeps committing, follower recovers via
snapshot and reconverges (same harness `RaftNodeFlakySlowNodeTest` uses), then compaction frees the
leader's deque — all green under the existing suite.

**Stage 6 — later (follow-ups, not this item):** chunking if payloads outgrow one message (already
reserved by `offset`/`done`); durable snapshot write — Note: Follow-up #2 (spill to durable store)
is a natural *consumer* of the Stage-1 artifact: "state → portable object" already exists, Stage
1 only adds the round-trip half.

## 5. Open questions (recorded, not blocking)

1. **Per-partition vs. flat snapshot.** `getSnapshot()` is per-partition; etcd flattens.
   Recommend keeping per-partition (matches `PartitionMap` internals; bridge adoption is a 1:1 copy).
2. **`getSnapshot()` global-version TODO** — resolved as designed in Stage 1 (carry `DataVersion`
   through, so version counters survive; the map's global counter is derived on read).
3. **Compaction threshold N.** etcd defaults to thousands of entries; for a cache workload a much
   smaller N is defensible since re-apply is cheap. Pick per Stage-5 tests.
4. **Snapshot retention after all followers catch up** (paper §7.1's "leader discards the snapshot
   when all followers have caught up"). The `RaftLogs` boundary must *stay* (the log still starts
   there); only the byte payload is droppable. Not worth the complexity until Stage 6.

## 6. Why no implementation in this pass

The signed-off plan defines item 4's delivery as *"an in-repo investigation + design document … A
partial implementation only if the full suite stays green; otherwise the design deliverable
stands."* Stages 3–5 rewrite the protocol paths that the 242-test suite (including the
timing-sensitive election/replication hardening) exists to guard; doing that unattended, after three
protocol-level changes in one branch, is the risk the plan explicitly declines to take. Stages 1–2
remain deliberately safe, isolated, and testable for a first implementation pass, with the staged
test plan above as the acceptance criteria.

> **Postscript (2026-11-23):** the user signed off the full implementation ("Yes do all of the
> stages. Test and commit as you go."), so this caution was honored by process, not outcome —
> §4 was executed stage by stage, each stage committed with a green `mvn -o -B test`, and the
> staged test plan of §4 served as the per-stage acceptance criteria. See the status table in the
> header for the commits.

## 7. Verification

**Investigation pass (2026-11-21)** — no source files changed: the full suite stayed at
**242 tests green, 0 failed, 1 pre-existing skip** (`mvn -o -B test`, green 2×:
`/tmp/opencode/mvn-prevote.log`, `/tmp/opencode/mvn-prevote-2.log`).

**Implementation pass (2026-11-23)** — each of stages 1→6(b) ran the full suite green before
commit (`/tmp/opencode/mvn-phase4-*.log`); final state at stage 6(b):
**320 tests green, 0 failures, 1 pre-existing skip** (`/tmp/opencode/mvn-phase4-final.log`), the
per-stage tests landing in `RaftLogsTest`, `RaftNodeBehaviorFollowerInstallSnapshotTest`
(install acceptance + completion save + seed + failing-store no-op, 16 tests),
`RaftNodeBehaviorLeaderInstallSnapshotTest` (transfer + compaction-persistence, 8 tests),
`InMemorySnapshotStoreTest` (7), `FileSnapshotStoreTest` (7), and the 3-node end-to-end
`RaftNodeSnapshotRecoveryTest` (1 — laggard recovers through the snapshot and the leader compacts
afterwards; stage 5 addendum, `9f0a705`), on top of the per-stage suite additions in
`StateMachineBridgeTest`, `RaftLogsTest`, and `WireSerializationTest`.
