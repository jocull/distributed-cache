# Scope & Roadmap — `finish` branch

Branch `finish` is cut from `rethink` @ `6219424` (identical to `origin/rethink`).
`master` is at `e99b869`, which predates the design-sketch additions in `rethink`.
Baseline on `finish`: **152 tests green** (`mvn -B test`), including 130 in `RaftNodeTest` and 5 in `RaftLogsTest`.

This document maps the six development phases in `README.md` to the current state of the code,
records the decisions taken while finishing phase 2 (the Raft → state-machine apply loop),
and lists the remaining items as explicit **cut-vs-finish** decisions so nothing is silently dropped.

---

## Phase status

### Phase 1 — Basic Raft protocol — ✅ done (with flagged deviations)

Present and proven by the test suite (`src/test/.../lib/raft/RaftNodeTest.java`):

- Leader elections with randomized timeouts; re-election after member failures
  (`testElection`, `testElectionWithFailure_oneNode`, `testElectionWithFailure_twoNodes`).
- Log replication via append/ack handshake with a mock network
  (`PassThruMiddleware` with `interrupt`/`restore`), including under term churn
  (`testLogReplication`, `testLogReplicationDuringTermChange`).
- Connection rewinding when a follower reports an earlier term/index
  (`RaftNodeBehaviorLeader.onAcknowledgeEntries`, ~`:220-228`).
- No serialization: plain message classes under `lib/raft/messages/`, as intended by README.

Flagged deviations / gaps (see Decisions below):

| Item | Location |
|---|---|
| Candidate votes on committed term/index, not last-log up-to-date (Raft §5.4.2) | `RaftNodeBehaviorCandidate.java:22-25` |
| ~~Leader's `AppendLog` subscriber was created but **never subscribed**~~ — **fixed in this branch**: subscribed in the leader ctor, so new entries push immediately via the `NEW_LOG` event instead of waiting up to one 50 ms heartbeat | `RaftNodeBehaviorLeader.java` ctor |
| ~~A 2-node cluster can never commit: majority is computed as `size/2+1` but the leader only counted follower acks~~ — **fixed in this branch**: the leader's own log now counts toward the commit majority | `ClusterTopology.java:34-36`, `RaftNodeBehaviorLeader.updateCommittedIndex` |
| ~~A 1-node cluster could never elect a leader (and therefore never commit or serve)~~ — **fixed in this branch**: the candidate recognizes its own vote as a majority when it is the only voter | `RaftNodeBehaviorCandidate.java` ctor |
| No snapshot install / log compaction (Raft §7) | `InstallSnapshotExample.java` is a sketch only |
| `disconnectFrom` throws `UnsupportedOperationException` | `RaftNodeImpl.java:166-169` |

### Phase 2 — Basic state machine fed by Raft — 🔨 completed in this branch

Present before this work:

- `lib/data/`: `PartitionMap` (hash-partitioned), `KeyMap`, `KeyValue`, `DataVersion` — with its own tests (`lib/data` coverage in `PartitionMapTest`).
- `lib/comm…nKeyDataInput` / `KeyDataOutput`.
- Raft emits `LogsCommitted(List<RaftLog<?>>)` on commit from **both** roles
  (leader `RaftNodeBehaviorLeader.java:162-163`, follower `RaftNodeBehaviorFollower.java:184-185`).

Missing before this work: **nothing consumed Raft logs.** There was no code path from a committed
`RaftLog` to `PartitionMap`. `Main.java` is a pure data-layer stress harness (`LargeThrashingTest`
pushing into a `PartitionMap` built on `Main.java:26-27`) and contains **no Raft cluster at all**;
nothing in the repo attached a map to a `RaftManager`'s bus.

This branch adds:

- `StateMachineBridge` (`lib/data/StateMachineBridge.java`): an `EventSubscriber` attached to the
  node event bus that applies each committed `RaftLog` whose entry is a `KeyDataInput` to the
  `PartitionMap`, tracking the highest applied index and skipping anything at or below it.
- End-to-end test in `RaftNodeTest` (leader `submit(KeyDataInput)` → committed → visible in the map;
  non-data entries ignored; followers' committed indexes caught up) plus a unit test class
  `StateMachineBridgeTest` (apply, duplicate-delivery idempotency, out-of-order skip, ignore non-data).

### Phase 3 — Node communication over network — ❌ not started

`ChannelMiddleware` is an interface; the only implementation is `PassThruMiddleware` (in-memory,
test-oriented with fault injection). No real transport, framing, or serialization of the
`Request`/`Response` envelope on the wire.

### Phase 4 — Non-partitioned distributed cache — ◐ partial

The cluster + replicated log + data layer exist in-process, but there is no client API that
routes reads/writes through the leader, no read-after-commit ordering guarantee exposed to users,
and `Main.java` does not exercise the cluster. Note: the map is internally partitioned
(from day one: `PartitionMap(partitionCount)`), so phases 4 and 5 blur — "single shard" effectively
means "one cluster".

### Phase 5 — Partitioned cache without repartition — ◐ partial

Partitions hash by key (`PartitionMap.getPartitionForKey`) and exist in every replica, but there is
no ownership model (which node owns which partition) and no rebalancing.

### Phase 6 — Repartitioning — ❌ not started

No live shard add/remove; consistent-hash migration does not exist.

---

## Design of the apply loop (what this branch adds)

- **One subscriber, both roles.** Leader and follower both emit `LogsCommitted` carrying the freshly
  committed `List<RaftLog<?>>` from `RaftLogs.commit()` (`RaftLogs.java:141-157`). A single
  `EventSubscriber` on that bus therefore covers the leader and follower paths.
- **Bus scoping.** `RaftManager` creates one shared `EventBus` (`RaftManager.java:12`) and passes it
  to every `RaftNodeImpl` it creates (`RaftNodeImpl.java:59` constructs `RaftLogs` on it). In
  production each node process has its own manager + bus + map + bridge, so exactly one bridge sees
  a node's commits. In the in-process test harness, one manager's bus carries **all** nodes' commits
  into one map — which is why the bridge must be duplication-safe (below), and it also means the
  "visible on leader and followers" guarantee is asserted there via each follower's *committed index*
  (their log contains the entry their own bridge would apply).
- **Idempotency.** `RaftLogs.commit()` moves `committedTermIndex` strictly forward and returns only
  the new range, so per-node batches never overlap. The bridge additionally records the highest index
  it has applied and skips `index <= lastApplied` — this covers the shared-bus harness where the same
  logical entry is committed (and therefore published) by multiple nodes, and any future redelivery.
- **Threading.** `EventBus.publish` is synchronous (`EventBus.java:17-19`), so apply runs on the
  publishing node's executor thread — no new threads or executors are introduced, per the
  single-threaded-per-node mailbox model.
- **Apply-then-acknowledge (behavioral fix).** `RaftLogs.commit()` used to complete the client
  futures itself (`.peek(CompletableRaftLog::commit)`), *before* the commit site published
  `LogsCommitted` — so a client could wake from `submit().get()` and immediately read a value
  older than its own write. `commit()` now only advances the commit index; both commit sites
  (leader `RaftNodeBehaviorLeader:162-168`, follower `RaftNodeBehaviorFollower:184-190`) follow
  the order: advance commit index → publish `LogsCommitted` (synchronous apply) →
  `RaftLogs.notifyCommitted(list)`, which completes the futures. A client's `submit().get()`
  therefore returns only *after* its entry is applied to the state machine — the standard
  Raft "commit → apply → reply" ordering (etcd applies before responding as well).
  `RaftLogsTest.testFutureRaftLogsCommit` locks in the contract (`commit()` alone must not
  complete the future); `RaftNodeTest.testStateMachineApply` asserts the guarantee
  deterministically (no timeout polls).
- **Payload scope.** Only `KeyDataInput` entries are applied (via `PartitionMap.setData`). `RaftLog`
  is generic (`RaftLog.java:5`); any other entry type (e.g. plain strings used by existing tests) is
  skipped. Deletion over Raft is **undefined** in the current command vocabulary — see Decisions.
- **Indices.** `TermIndex.EPOCH = (0, 0)` (`TermIndex.java:6`), so real log indices start at 1 and
  `lastAppliedIndex` starting at 0 is correct.

## Behavior fixes committed in this branch

These are behavioral (not merely additive) changes, made because they were required for
"working distributed cache" — a one- or two-node cluster was unusable without them.

- **Leader counts its own log toward the commit majority** — `RaftNodeBehaviorLeader.updateCommittedIndex`
  now seeds the replica-index list with `self.logs.getCurrentTermIndex()` before taking the
  majority element. Majority = `clusterCount/2 + 1` over the whole cluster (leader included), which
  is the standard Raft definition. Previously only followers' acks counted, so a 2-node cluster
  could replicate but never commit, and a 1-node cluster never committed at all. A fresh leader's
  `INITIAL` call is a no-op: its own index = `EPOCH`, and `EPOCH.compareTo(EPOCH) == 0` fails the
  strictly-forward check.
- **Single-node self-election** — `RaftNodeBehaviorCandidate` ctor: after adding its own vote, if
  that already meets the majority (only possible in a 1-node cluster — for any n ≥ 2 the own vote
  is 1 < n/2+1), the node is scheduled to convert to leader asynchronously on its `nodeExecutor`
  and skips the vote-request send / election-timeout scheduling. The async deferral matters: the
  timeout-driven entry path re-enters the constructor on the same thread, so converting to leader
  inline would break `RaftNodeImpl.convertToCandidate`'s cast and leave the constructor running on
  a replaced behavior. Without this, a 1-node cluster looped `candidate → candidate → …` forever
  and `submit()` (which requires a leader) could never execute.
- **NEW_LOG subscriber now subscribed** — `RaftNodeBehaviorLeader` ctor subscribes
  `entriesSubscriber`, so a new entry drives `appendEntries(NEW_LOG)` immediately instead of
  waiting up to one 50 ms heartbeat. `EventBus.publish` is synchronous on the publisher's thread,
  so no new executors are introduced. The ack-window gate in `shouldSendNextEntries` is untouched.
- **Empty-cluster commit shortcut** — in `appendEntries`, if `self.activeConnections.isEmpty()`
  (a true 1-node cluster; `activeConnections` is append-only until `disconnectFrom` exists), the
  leader calls `updateCommittedIndex()` directly and skips the heartbeat timer, instead of
  busy-rescheduling at a 0 ms delay that keeps re-firing against an empty connection list.

## Decisions — cut-vs-finish

Cut (documented here, intentionally **not** fixed by this branch):

1. **Candidate log up-to-date election rule** — `RaftNodeBehaviorCandidate.java:22-25` sends the
   *committed* term/index in `VoteRequest`, not the last-entry term/index required by Raft §5.4.2.
   Changing vote behavior is an election-semantics change and was deferred pending an explicit
   decision rather than bundled with the apply loop.

   (Superseded-in-part by the single-node self-election fix below: the candidate now converts to
   leader as soon as its own vote reaches a majority — a condition that is only *reachable* in a 1-node
   cluster — without waiting for a vote response.)
2. **Snapshots / log compaction** — `InstallSnapshotExample.java` and the `proto/raft/*` package
   (`src/main/java/.../proto/raft/`, including its own sketch test) are design sketches and remain
   as working notes. They are **not build targets** and must not be deleted without sign-off.
   `RaftLogs.rollback()` only trims uncommitted logs; committed logs are never compacted.
3. **`disconnectFrom`** — `RaftNodeImpl.java:166-169` throws `UnsupportedOperationException`;
   node removal is not part of phase 2. (Consequence: the downed-follower quorum path — a leader
   with a live follower disconnected — is untestable until this exists; the quorum fix above was
   verified with 1- and 2-node *up* clusters instead.)
4. **`Replica` stub** — `lib/replication/Replica.java:15-19` is an empty `SetDataEvent` subscriber
   ("Do something here!"). It represents the *map → replication* direction; the current design
   feeds the map *from* Raft, so this stays a stub.
5. **Deletion over Raft** — there is no log entry type distinguishing "set" from "delete"
   (`KeyDataInput` has no delete flag). The bridge applies `setData` only; a proper delete command
   (e.g. a dedicated `DataDelete` entry) is a follow-up, not guessed semantics.
6. **`RaftLogs` performance** — `RaftLogs.java:68-69` notes walk-the-deque scaling; out of scope here.
7. **`Main.java`** — left as the data-layer stress harness; it never had a cluster. Wiring a
   production-style demo (one process = one manager + one map + one bridge) is a phase 3/4 concern.

## Test evidence

Counts are `mvn -B test` totals over the whole module. `RaftNodeTest` is parameterized (5
iterations per `@ParameterizedTest`), so the test total moves with it.

- Baseline (branch start, before any change): **152 tests green**, incl. 130 `RaftNodeTest`
  iterations.
- After the apply loop + hardening (commits `9a15525`, `eece74f`): **163 tests green**, adding
  `StateMachineBridgeTest` (6) and `RaftNodeTest.testStateMachineApply` (5 iterations).
- After the behavior fixes (this commit): **173 tests green**, adding two `RaftNodeTest`
  regression tests × 5 iterations:
  - `testSingleNodeElectionCommitApply` — a 1-node cluster self-elects a leader, commits a
    `KeyDataInput` by its own log alone, and the bridge applies it: `submit().get()` returns,
    `bridge.getLastAppliedIndex()` ≥ the committed index, `PartitionMap` holds the value, and the
    `DataVersion` is exactly 1 (applied exactly once, no duplicate application).
  - `testTwoNodeCommitApply` — a 2-node cluster elects a leader, replicates the entry to the
    follower, and commits it with leader + follower = the quorum of 2; same apply/version
    assertions, plus the follower's log caught up to the committed index.
- Result: **173 tests green** (`mvn -B test`), all 152 baseline tests still passing, including
  the existing `RaftNodeTest` suite now at 145 iterations (130 baseline + 5 apply-loop + 10 new).
  The apply-then-acknowledge ordering is what lets these assert the bridge index and map state
  *immediately* after `submit().get()` returns, deterministically (no timeout polls).
- `RaftLogsTest` is the regression guard for the two-phase commit/acknowledge contract.
- `testSingleNodeElectionCommitApply` / `testTwoNodeCommitApply` are the regression guards for the
  single-node election and leader-counts-quorum behavior fixes.
- Existing `testLogReplication*` remain the replication-regression guard.
