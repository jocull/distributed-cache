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
| Leader's `AppendLog` subscriber is created but **never subscribed** — log push rides the 50 ms heartbeat instead of the `NEW_LOG` event | `RaftNodeBehaviorLeader.java:40-47` (only refs: decl, ctor, `closeInternal:55`) |
| A 2-node cluster can never commit: majority is computed as `size/2+1` but the leader only counts follower acks | `ClusterTopology.java:34-36`, `RaftNodeBehaviorLeader.updateCommittedIndex:146-168` |
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
- **Payload scope.** Only `KeyDataInput` entries are applied (via `PartitionMap.setData`). `RaftLog`
  is generic (`RaftLog.java:5`); any other entry type (e.g. plain strings used by existing tests) is
  skipped. Deletion over Raft is **undefined** in the current command vocabulary — see Decisions.
- **Indices.** `TermIndex.EPOCH = (0, 0)` (`TermIndex.java:6`), so real log indices start at 1 and
  `lastAppliedIndex` starting at 0 is correct.

## Decisions — cut-vs-finish

Cut (documented here, intentionally **not** fixed by this branch):

1. **Candidate log up-to-date election rule** — `RaftNodeBehaviorCandidate.java:22-25` sends the
   *committed* term/index in `VoteRequest`, not the last-entry term/index required by Raft §5.4.2.
   Changing vote behavior is an election-semantics change and was deferred pending an explicit
   decision rather than bundled with the apply loop.
2. **Dead leader `NEW_LOG` subscriber** — `RaftNodeBehaviorLeader.java:40-47` creates
   `entriesSubscriber` and unsubscribes it in `closeInternal`, but never subscribes it. Replication
   still works because the 50 ms heartbeat (`:23`) pushes entries, at the cost of up to one
   heartbeat of latency per entry. Fixing it properly requires scoping `AppendLog` events per node
   (the manager bus is shared across nodes — `RaftManager.java:12,30`); deferred as a follow-up.
3. **2-node clusters never commit** — majority is `size/2+1` (`ClusterTopology.java:34-36`) but the
   leader's `updateCommittedIndex` (`RaftNodeBehaviorLeader.java:146-168`) only counts followers'
   acks, not itself. A 2-node cluster can replicate but never commit. Behavioral change; deferred.
4. **Snapshots / log compaction** — `InstallSnapshotExample.java` and the `proto/raft/*` package
   (`src/main/java/.../proto/raft/`, including its own sketch test) are design sketches and remain
   as working notes. They are **not build targets** and must not be deleted without sign-off.
   `RaftLogs.rollback()` only trims uncommitted logs; committed logs are never compacted.
5. **`disconnectFrom`** — `RaftNodeImpl.java:166-169` throws `UnsupportedOperationException`;
   node removal is not part of phase 2.
6. **`Replica` stub** — `lib/replication/Replica.java:15-19` is an empty `SetDataEvent` subscriber
   ("Do something here!"). It represents the *map → replication* direction; the current design
   feeds the map *from* Raft, so this stays a stub.
7. **Deletion over Raft** — there is no log entry type distinguishing "set" from "delete"
   (`KeyDataInput` has no delete flag). The bridge applies `setData` only; a proper delete command
   (e.g. a dedicated `DataDelete` entry) is a follow-up, not guessed semantics.
8. **`RaftLogs` performance** — `RaftLogs.java:68-69` notes walk-the-deque scaling; out of scope here.
9. **`Main.java`** — left as the data-layer stress harness; it never had a cluster. Wiring a
   production-style demo (one process = one manager + one map + one bridge) is a phase 3/4 concern.

## Test evidence

- Baseline: 152 tests green before changes.
- Added: `lib/data/StateMachineBridgeTest` (6 unit tests: apply, non-data acknowledgment,
  duplicate-delivery skip, out-of-order skip, unrelated events, close/unsubscribe) and
  `RaftNodeTest.testStateMachineApply` (5 integration iterations: 3-node cluster, committed
  `KeyDataInput` visible in the map with the latest write and a state-machine version of exactly
  2 -- both writes applied exactly once on the shared bus, non-data entry acknowledged without
  blocking later entries, bridge applied index >= last committed index, followers' logs caught up).
- Result: **163 tests green** (`mvn -B test`), all 152 baseline tests still passing.
  Note: the integration test polls the bridge's applied index rather than asserting it
  immediately -- `RaftLogs.commit()` completes the client future via `completeAsync(commonPool)`
  before the leader publishes `LogsCommitted`, so the test thread can wake before the bridge
  has applied the final entry.
- Existing `testLogReplication*` remain the replication-regression guard.
