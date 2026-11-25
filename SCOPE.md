# Scope & Roadmap — `finish` branch

Branch `finish` is cut from `rethink` @ `6219424` (identical to `origin/rethink`).
`master` is at `e99b869`, which predates the design-sketch additions in `rethink`.
Baseline on `finish`: **152 tests green** (`mvn -B test`), including 130 in `RaftNodeTest` and 5 in `RaftLogsTest`.

JVM (this branch): **Java 17 is the floor.** `pom.xml` uses `maven.compiler.release=17`, so the
build works on any JDK 17 or newer (a newer compiler JDK is fine), but the code is enforced to
use only APIs that exist in 17 — the produced bytecode (major 61) runs unchanged on a Java 17
runtime. Previously: Java 11.

This document maps the six development phases in `README.md` to the current state of the code,
records the decisions taken while finishing phase 2 (the Raft → state-machine apply loop),
and lists the remaining items as explicit **cut-vs-finish** decisions so nothing is silently dropped.

---

## Signed-off work plan (2026-11-20)

Sign-off from the user (2026-11-20, unattended run): *"You should first begin by doing
DELETE. Then do TTL/EXPIRE. Then do Pre-vote. Then look into doing snapshots/log compaction.
Write down your plan, and then begin executing until you reach your goal unattended."*
The first three items are approved implementation work — Pre-Vote is a `src/main` protocol
change and is **explicitly signed off**; item 4 is an investigation/design deliverable
("look into"), not necessarily a full implementation. The order is fixed. Commit and test
as it goes (green `mvn -o -B test` at each step). **Do not push.**

| # | Item | Closes | Delivery |
|---|---|---|---|
| 1 | **DELETE over Raft** | Decision #5, Follow-up #1 | A new log entry `KeyDataDeleteInput` (key only) in `lib/communication`, modeled on `KeyDataInput`; the bridge applies it via the existing, tested `PartitionMap.deleteData` (deleting an absent key is a no-op, so replay is idempotent — safe under Raft); `DistributedCache.delete(key)` routes through the leader exactly like `put`. Tests: bridge-level apply in `StateMachineBridgeTest`; facade e2e in `DistributedCacheTest` (put → delete → get empty, and re-put after delete visible with the next version) |
| 2 | **TTL / EXPIRE** | Follow-up #3 | A replicated `EXPIRE` command: `KeyDataExpireInput(key, ttl)` routed through the leader; the bridge resolves the expiration at apply time against an injectable `Clock` held by `PartitionMap` (default `Clock.systemUTC()`, test-overloaded constructor) and stores it per entry; `getData` serves expired entries as absent (checked read); `sweepExpired()` removes all expired entries and publishes `DeleteDataEvent` per removal; `DistributedCache.expire(key, ttl)`. Expiration resolves independently on each node within milliseconds of each other — recorded choice, acceptable for a cache; deterministic tests via clock injection |
| 3 | **Pre-Vote** | Follow-up #10 (thesis §9.6) | A pre-vote phase **before** any term bump: on its election timeout, a follower — *still a follower at its current term, granting nothing, writing nothing* — proposes its term+1 to the cluster and waits for a **hypothetical majority**; only once granted may it convert to candidate and increment its term. A higher-term response steps it down. Effect: a degraded member that repeatedly times out no longer burns real terms denying/being denied by healthy members — the term churn that the slow-follower tests observed goes away. `RaftNodeFlakySlowNodeTest` additionally pins catch-up **during** the degradation window (bounded cluster term) — the availability Pre-Vote exists for. New `PreVoteRequest`/`PreVoteResponse` messages + candidate/follower logic + deterministic grant/deny unit tests |
| 4 | **Snapshots / log compaction** | Decision #2, Follow-up #5 | "Look into": an in-repo investigation + design document grounded in the actual code — `RaftLogs` (deque log, `rollback()` trims **uncommitted** only, committed entries are never compacted), the `InstallSnapshotExample` sketch and the `proto/raft/*` working notes (do-not-modify), Raft paper §7, Ongaro's thesis, and etcd practice — with a staged implementation plan. A partial implementation only if the full suite stays green; otherwise the design deliverable stands |

As each item lands, this document is updated in place: the Decision / Follow-up entries flip
to done (with commit refs), and the Test-evidence section gains the per-item run counts.

**Status (2026-11-21):**

| # | Status |
|---|---|
| 1 DELETE | ✅ done — commit `40700d0`, 213 tests green |
| 2 TTL/EXPIRE | ✅ done — commit `f8e901d`, 232 tests green |
| 3 Pre-Vote | ✅ done — commit `f35e60d`, 242 tests green |
| 4 Snapshots | ✅ done (2026-11-23) — investigated 2026-11-21 (`SNAPSHOTS.md`), then every stage implemented: `47d9f9f` → `e9102f1` → `d658475` → `3fd7e6c`/`9f0a705` → `35f4ec0` → `stage 6(b)` (stages 1–6(b)); suite at close: 320 green |

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
| ~~Candidate votes on committed term/index, not last-log up-to-date (Raft §5.4.2)~~ — **fixed in this branch**: the candidate's `VoteRequest` reports the LAST log entry's term/index (Raft §5.4.1), the vote step-down paths no longer roll back uncommitted tails, and the candidate's vote tally counts granted responses only; guarded by `testUpToDateRuleStaleCandidateLoses` and the liveness discriminator `testUpToDateRuleUncommittedTailLoses` | `RaftNodeBehaviorCandidate.java` ctor, `RaftNodeImpl` vote step-downs, `RaftNodeBehaviorCandidate.onVoteResponse` |
| ~~Leader's `AppendLog` subscriber was created but **never subscribed**~~ — **fixed in this branch**: subscribed in the leader ctor, so new entries push immediately via the `NEW_LOG` event instead of waiting up to one 50 ms heartbeat | `RaftNodeBehaviorLeader.java` ctor |
| ~~A 2-node cluster can never commit: majority is computed as `size/2+1` but the leader only counted follower acks~~ — **fixed in this branch**: the leader's own log now counts toward the commit majority | `ClusterTopology.java:34-36`, `RaftNodeBehaviorLeader.updateCommittedIndex` |
| ~~A 1-node cluster could never elect a leader (and therefore never commit or serve)~~ — **fixed in this branch**: the candidate recognizes its own vote as a majority when it is the only voter | `RaftNodeBehaviorCandidate.java` ctor |
| ~~No snapshot install / log compaction (Raft §7)~~ — **investigated 2026-11-21 (item 4), implemented 2026-11-23:** full Raft §7 delivered per `SNAPSHOTS.md` stages 1→6(b), commits `47d9f9f`…`stage 6(b)`; Follow-up #5 closed | `InstallSnapshotExample.java` sketch (do-not-modify, untouched); plan + evidence in `SNAPSHOTS.md`; implementation in `lib/raft` |
| ~~`disconnectFrom` throws `UnsupportedOperationException`~~ — **resolved 2026-11-24 (infra-debt item 4)**: `RaftNode#disconnectFrom` tears down a live link (local cleanup only, idempotent, no auto-reconnect); the downed-follower quorum path it blocked is now pinned (`RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable`) and exercised over the real wire (`RaftNodeDisconnectTest`) | `RaftNode#disconnectFrom`, `RaftNodeImpl#disconnectFromInternal`, `ChannelSide#close` + both middleware impls |

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

### Phase 3 — Node communication over network — ✅ done in this branch (staged A→D, 2026-11-21)

`ChannelMiddleware` is an interface; the only implementation is `PassThruMiddleware` (in-memory,
test-oriented with fault injection). No real transport, framing, or serialization of the
`Request`/`Response` envelope on the wire.

**Direction (user, 2026-11-21):** this is the focus now — implement a real socket
`ChannelMiddleware` and build the integration-test suite that runs an actual 3-node cluster:
mutate it, take nodes down/restore them, and run real request/response against it, re-proving
the behaviors today covered only by mocked (PassThru) tests. Snapshots/log-compaction
(`SNAPSHOTS.md`) are explicitly **deferred** per the same direction — "we'll do snapshots later"
— and remain the recorded backlog, not part of this work. (That deferral was later lifted: the
user signed off the full implementation and every stage landed 2026-11-23 — see Decision #8's
addendum and Follow-up #5.)

The design is documented in **`NETWORKING.md`** (wire format, `SocketChannelMiddleware`,
fault-injection parity with `PassThruMiddleware`, node take-down/restart semantics, invariants
N1–N6, the 7 IT scenarios, and staged implementation A→D). All four stages are delivered in
this branch — each committed only with a green `mvn -o -B test`, and each stage re-proves the
behaviors today covered only by the PassThru-mocked `RaftNodeTest` tests over a real wire:

- **Stage A** (commit `6dc8a1e`, 254 green) — the wire surface implements
  `java.io.Serializable`, zero new dependencies (Decision #9): the 11 Raft message types, the
  3 `KeyData*Input` payload types, and the topology types (`NodeAddress`, `NodeIdentifier`,
  `TermIndex`); guarded by `WireSerializationTest` (12 tests: round-trip fidelity +
  null/boundary rejection).
- **Stage B** (commit `7a7eb1f`, 260 green) — `SocketChannelMiddleware`, the second (real)
  `ChannelMiddleware` implementation: real `127.0.0.1`/ephemeral-port channels, 4-byte-BE
  length framing (payload-neutral), a server-side handshake that resolves the peer's `RaftNode`
  from its announced address, and PassThru-parity per-node `interrupt`/`restore` (N1–N3);
  guarded by `SocketChannelMiddlewareTest` (6 tests).
- **Stage C** (commit `b54199b`, 266 green) — `ClusterKit`, a production-shaped 3-node harness
  (each node its own middleware on an ephemeral port, own `RaftManager`/`EventBus`, own
  `PartitionMap`/`DistributedCache`), plus ITs 1–6: leader election, put/get (apply-then-
  acknowledge visibility), delete/expire e2e, and partition-restore of a follower (N4, quorum
  holds on 2-of-3) and of the leader (N5, higher-term re-election, old leader steps down or
  re-wins at a higher term, one leader overall); guarded by `RealNetworkClusterTest` (6 tests).
- **Stage D** (commit `f9b2fde`, 267 green) — N6, the last scenario: `ClusterKit.hardStop(i)`
  kills a slot in crash order (facade unsubscribes → terminal node stop → idempotent socket
  close; nothing resumable) and `restart(i)` brings the same slot back with process-restart
  semantics (fresh ephemeral wire address, empty log at term 0, same Raft id, fresh state
  machine; the survivors keep their bus and map but their facades are rebuilt over a fresh
  node list). IT 7: v1 applied on all three → hard-stop a **follower** → v2 commits on a
  2-of-3 quorum → the stopped node's view stays stale at "v1" → restart → the surviving
  leader's continuation push replays v1-then-v2 into the fresh node's new map → all three
  local maps hold "v2" at version 2 exactly → exactly one leader, the pre-stop survivor
  (7 tests in the class).

Snapshots/log compaction were **explicitly deferred** by the same direction ("we'll do
snapshots later") and remain the `SNAPSHOTS.md` backlog (Decision #8). — deferral lifted 2026-11-23:
the user signed off all snapshot stages and they were implemented in full (see the Decision #2
addendum, the Decision #8 addendum, and Follow-up #5; final suite 320 green).

### Phase 4 — Non-partitioned distributed cache — ✅ client API done in this branch

The cluster + replicated log + data layer existed in-process; this branch adds the client API
on top:

- `lib/cache/DistributedCache`: `put(key, data)` finds the current leader among the supplied
  nodes and routes the `KeyDataInput` through `RaftNode#submit`; `get(key)` serves the local
  `PartitionMap` view. Thanks to apply-then-acknowledge, `put`'s future completes only after
  the entry is committed by a majority **and** applied to the local state machine — so a `get`
  immediately after `put` sees the value (read-your-writes, no client-side polling). The cache
  also wires its own `StateMachineBridge` between the cluster bus and the local map, so a
  consumer supplies the nodes and the map and gets a working cache with no extra plumbing.
- End-to-end tests in `lib/cache/DistributedCacheTest`: `put`/`get` through a 3-node cluster
  and through a single-node cluster (each ×5), asserting immediate visibility and exactly-once
  application (version checks), plus the empty-node-list constructor guard.

Deliberately not in scope for phase 4 (see Decisions / Phase 3):

- Real network transport (`PassThruMiddleware` is in-memory only) — leader routing in
  `DistributedCache` is in-process; the supplied nodes must be directly reachable. Routing over
  a persistent connection follows with phase 3 — **delivered**: the Phase 3 stages above
  (`SocketChannelMiddleware` + the 7-scenario real-socket IT suite, IT 2–4 exercising
  `DistributedCache`'s leader-routed put/delete/expire over the wire).
- ~~Deletion over Raft (Decision #5)~~ — **done** under the Signed-off work plan, item 1
  (commit `40700d0`): `KeyDataDeleteInput` entry, bridge apply, `DistributedCache.delete()`.
- ~~EXPIRE / TTL (Follow-up #3)~~ — **done** under the Signed-off work plan, item 2
  (commit `f8e901d`): `KeyDataExpireInput` entry, checked-read + sweep expiration,
  `DistributedCache.expire(key, ttl)`.
- ~~Downed-follower quorum validation (Decision #3, blocked on `disconnectFrom`)~~ — **done 2026-11-24
  (infra-debt item 4)**: with `disconnectFrom` landed, the leader's commit path became reachable
  from an isolated multi-node cluster — and the commit gate already counts the leader's own vote
  against the TOPOLOGY's majority (`RaftNodeBehaviorLeader.updateCommittedIndex`), so a 2-node
  leader with its only peer disconnected holds 1-of-2 and cannot commit; pinned by
  `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable`, exercised over real sockets by
  `RaftNodeDisconnectTest`.

Note: the map is internally partitioned (from day one: `PartitionMap(partitionCount)`), so
phases 4 and 5 blur — "single shard" effectively means "one cluster".

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
- **Payload scope.** The bridge applies, in explicit `instanceof` branches (each entry type must
  be named), `KeyDataInput` → `PartitionMap.setData` (SET), `KeyDataDeleteInput` →
  `PartitionMap.deleteData` (DELETE, absent-key no-op ⇒ idempotent under Raft — work plan item 1,
  commit `40700d0`), and `KeyDataExpireInput` → `PartitionMap.expireData` (EXPIRE/TTL — work plan
  item 2, commit `f8e901d`). `RaftLog` is generic (`RaftLog.java:5`); any other entry type
  (e.g. plain strings used by existing tests) is skipped. The bridge advances
  `lastAppliedIndex` for **every** entry type it applies, which is why the DELETE and EXPIRE
  branches are explicit rather than folded into a catch-all.
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
  (today: a true 1-node cluster, or a multi-node leader that has `disconnectFrom`'d every peer),
  the leader calls `updateCommittedIndex()` directly and skips the heartbeat timer, instead of
  busy-rescheduling at a 0 ms delay that keeps re-firing against an empty connection list.
  Safety after `disconnectFrom` (2026-11-24): the gateway was already topology-safe —
  `updateCommittedIndex` commits only once the leader's own vote plus follower acks reach the
  TOPOLOGY's majority (`ClusterTopology.getMajorityCount()`), so a 2-node leader with its only
  peer disconnected holds 1-of-2 and cannot commit. Now pinned by
  `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable`, with `RaftNodeDisconnectTest` covering
  the same path over the real wire plus re-joining.
- **A vote denial does not burn the node's one vote** (commit `77638bd`, sign-off'd protocol fix)
  — a follower that denied a stale candidate was writing `votedForNodeId` to its own id, which
  latched the once-per-term guard for the whole term; a *fresher* candidate arriving later in the
  same term then got no response, costing an entire empty term to elect the freshest node.
  `RaftNodeBehaviorFollower.onVoteRequest` now records the vote only on a grant (denial leaves
  `votedForNodeId` null, so a same-term fresher candidate can still win on the next RPC). Per Raft
  §5.4.1 a node grants at most one vote per term — the fix preserves that bound while restoring
  the freshness ordering Raft's election timeout randomization is meant to preserve.
  Test seam: `RaftNodeImpl.currentReceiver()` (package-private, for tests).

## Decisions — cut-vs-finish

Cut (documented here, intentionally **not** fixed by this branch — except #1, which was
subsequently decided, fixed, and regression-tested):

1. **Candidate log up-to-date election rule** — *(resolved in this branch: commits
   `6b33a9c`, `2c4dd89`, `1dc88a6`)*. The original cut: `RaftNodeBehaviorCandidate.java:22-25`
   sent the *committed* term/index in `VoteRequest`, not the last-entry term/index required by
   Raft §5.4.2. Changing vote behavior is an election-semantics change and was first deferred
   pending an explicit decision rather than bundled with the apply loop.

   (In a 1-node cluster the single-node self-election fix above means the candidate converts to
   leader on its own vote and this rule is never consulted there; it still governed 2+-node
   elections.)

   **Resolution.** The candidate now reports its LAST log entry's term/index (Raft §5.4.1), and
   the voting path that the fix exposed was hardened so the rule is actually honored:
   `convertToFollowerForVote` no longer rolls back uncommitted tails on the `onVoteRequest`
   step-down paths, the candidate's vote tally counts granted responses only
   (`isVoteGranted()` guard in `onVoteResponse`), and `PassThruMiddleware.interrupt`/`restore`
   are per-node. Since committed entries are a subset of the log, the old committed-based
   report was strictly more conservative — a liveness deviation, never a safety one.
   Guarded by `testUpToDateRuleStaleCandidateLoses` (voter-side rule; the test itself is
   NOT the discriminator, since there committed == last for every voter) and
   `testUpToDateRuleUncommittedTailLoses` (the liveness discriminator, proven by reverting
   the candidate to the committed report and watching the split starve — see Test evidence).
2. **Snapshots / log compaction** — *(investigation + design delivered 2026-11-21 as item 4:
   `SNAPSHOTS.md`)*. `InstallSnapshotExample.java` and the `proto/raft/*` package
   (`src/main/java/.../proto/raft/`, including its own sketch test) are design sketches and remain
   as working notes. They are **not build targets** and must not be deleted without sign-off.
   `RaftLogs.rollback()` only trims uncommitted logs; committed logs are never compacted.
   **Resolution (2026-11-21).** Item 4 delivered per the signed-off plan as the investigation/design
   deliverable — `SNAPSHOTS.md` records the full ground truth (§1: the deque log with its `EPOCH_LOG`
   seed and compaction-absent `rollback()`, the uncalled `PartitionMap.getSnapshot()`, the absent
   snapshot message/handler/trigger, the catch-up path with no escape hatch), the §7/§5.4.1/etcd gap
   analysis (§2), the five safety invariants S1–S5, and a five-stage implementation plan ordered by
   risk (§3–§4). Per the plan's clause, no partial implementation was taken in the unattended pass;
   Stages 1–2 are recorded as the safe, isolated first implementation step.
   **Addendum (2026-11-23).** The user signed off the full implementation ("Yes do all of the
   stages. Test and commit as you go."), so the staged plan was executed in full — each stage
   committed with a green `mvn -o -B test` (per-stage detail in `SNAPSHOTS.md`'s header table
   and §7): stage 1 state capture & wholesale replacement — `Snapshot` capture/restore + bridge
   adopt (`47d9f9f`); stage 2 `RaftLogs` snapshot boundary + `compact()` closing the
   `containsStartPoint` TODOs (`a96abc8`); stage 3 `InstallSnapshot`/`Response` messages,
   wire-shaped per `SNAPSHOTS.md` §3 (`e9102f1`); stage 4 follower install behind the S2
   progress-only guard — single in-progress assembly, on `done` decode → publish
   `SnapshotInstalled` → `logs.compact` → reply (`d658475`); stage 5 leader laggard
   `SnapshotTransfer` (pins ONE serialization; `SNAPSHOT_CHUNK_BYTES = 64*1024L`) + compaction
   trigger behind the S1 bound, with the per-node snapshot-supplier addendum (`3fd7e6c`,
   `9f0a705`); stage 6(a) chunked transfer — leader resumes at the follower's returned offset
   (`35f4ec0`); stage 6(b) durable `SnapshotStore` — `save(TermIndex boundary, byte[] payload)`
   with the boundary IN the API, `InMemorySnapshotStore` and `FileSnapshotStore`
   (directory-anchored atomic save), save-on-compact (leader) / save-on-install (follower) with
   a failed save aborting BEFORE the trim, and `RaftNodeImpl` seeding in `startInternal()`
   before log replay — null/empty/throwing/corrupt stores warn and skip, startup is never
   blocked (`stage 6(b)`). Final suite: **320 tests green, 0 failed, 1 pre-existing skip**
   (`/tmp/opencode/mvn-phase4-final.log`). Closes Decision #2 in full and Follow-up #5.
3. **`disconnectFrom`** — *(resolved: infra-debt item 4, 2026-11-24)*. The original cut:
   `RaftNodeImpl.java:166-169` threw `UnsupportedOperationException`; node removal was not part
   of phase 2. (Consequence: the downed-follower quorum path — a leader with a live follower
   disconnected — is untestable until this exists; the quorum fix above was verified with
   1- and 2-node *up* clusters instead.) **Resolution.** `RaftNode#disconnectFrom(NodeConnectionOutbound)`
   (javadoc: local cleanup only, idempotent, no auto-reconnect) removes the target link from the
   connection list and closes the `ChannelSide` — a new abstract `ChannelSide#close`, implemented
   per middleware: `SocketChannelSide#close` drops the connection and closes the socket (the peer
   sees EOF and self-heals); `PassThruChannelSide#close` seals both sides — a `closed` seal that is
   independent of the `interrupted` flag, so a sealed link cannot be re-opened even after
   `restoreFor`. `NodeConnectionOutboundImpl#close` delegates to the channel, and the public
   entry `RaftNodeImpl#disconnectFrom` runs `disconnectFromInternal` on the node executor,
   mirroring `connectWithTopology`. Two new tests: `RaftNodeDisconnectTest` (3-node loopback
   socket cluster — a 2-of-3 write commits while one follower is disconnected and frozen at v1,
   idempotent re-disconnect is a no-op, `connectWithTopology()` re-dials and the target catches up
   to v2) and `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable` (2-node quorum pin — v1
   commits with the peer up, `disconnectFrom` isolates the leader, a new v2 write stays pending
   and the state machine holds only v1).
4. **`Replica` stub** — *(resolved: infra-debt item 2, removed as dead code, 2026-11-24)*.
   The original cut: `lib/replication/Replica.java:15-19` was an empty `SetDataEvent`
   subscriber ("Do something here!"). It represented the *map → replication* direction; the
   actual design feeds the map *from* Raft. **Resolution.** Verified it was never alive
   anywhere — no
   `new Replica(` and no `lib.replication` import in `src/` outside the class itself — so the
   "keep the stub" framing had no subject left: the class and its package were deleted, and
   the map→replication direction it sketched is still expressed by the real apply path (the
   sealed `StateMachineBridge` intake). No test touches it; the EventBus 6(y) pins cover the
   identical `instanceof`-filtering subscriber shape (SNAPSHOTS.md §6(y) addendum).
5. **Deletion over Raft** — *(resolved: Signed-off work plan item 1, commit `40700d0`)*.
   The original cut: there is no log entry type distinguishing "set" from "delete"
   (`KeyDataInput` has no delete flag). The bridge applies `setData` only; a proper delete
   command (e.g. a dedicated `DataDelete` entry) is a follow-up, not guessed semantics.
   **Resolution.** `KeyDataDeleteInput` (key-only, modeled on `KeyDataInput`) is the replicated
   DELETE command; the bridge applies it via the existing, tested `PartitionMap.deleteData`
   (absent-key delete is a no-op, so replay is idempotent under Raft);
   `DistributedCache.delete(key)` routes through the leader exactly like `put`, and its future
   completes only after the entry is committed **and** applied (apply-then-acknowledge).
   Guarded by 3 bridge-level tests (apply, absent-key no-op, re-delivery idempotency) and
   DELETE e2e in `DistributedCacheTest` (3-node and single-node).
6. **`RaftLogs` performance** — *(resolved: infra-debt item 1, 2026-11-24)*. The original
    cut: `appendLogInternal` walked the whole deque from the seed for every append, O(log
    length) per append. **Resolution.** `appendLogInternal(TermIndex, T)` now takes an
    O(1) tail fast path: the deque is index-ordered with non-decreasing terms along it
    (invariant over every successful mutation, pinned by the existing guard tests), so when
    the new index is beyond the last record's, that one record decides the append — replacement
    is structurally impossible and the "earlier record outranks" check reduces to the last
    record outranking. Mid-log appends (idempotent re-append, §5.3 conflict replacement, a gap)
    keep the original verbatim walk with identical ISE semantics. The profiling TODO at
    `RaftLogs.java:119-121` is closed. Guarded by `testLargeLogAppendAtScale` (100k tail appends
    + deep mid-log re-append / replacement / commit / compaction, ~7s — O(n), not O(n²)); the
    6r/6s/6t guard suite is untouched and green.
7. **`Main.java`** — *(resolved: infra-debt item 3, rewired as a live cluster demo, 2026-11-24)*.
   It was the data-layer stress harness (`LargeThrashingTest` pushing into one shared
   `PartitionMap`; no Raft at all); it now brings up an N-node (default 3) cluster on loopback
   over real `SocketChannelMiddleware` sockets — per-node middleware/bus/node/map/facade, full
   peer topology, then the `connectWithTopology` → `start` bootstrap — and drives a
   put/get/expire/delete smoke against `DistributedCache`, asserting convergence on every node
   before tearing down in reverse construction order. The now-orphaned `LargeThrashingTest`
   stress harness was removed along with it. In the course of running it, the
   `@SpringBootApplication` shell came off too: the `spring-boot-starter` 2.4.0 it depended on
   (Spring 5.3.1) cannot parse this project's Java-17 bytecode, so a Spring context could never
   have started here — the wrapper was effectively dead on arrival. `Main` is now a plain
   `static main`, and `pom.xml` depends on `logback-classic` directly (version still pinned by
   the `spring-boot-starter-parent` BOM).
8. **Phase 3 — real-network ITs are the immediate focus; snapshots deferred** *(decided
   2026-11-21 — user direction)*. "This is a good place to start testing full network
   implementations": scope + log first (`NETWORKING.md`, committed alongside this entry), then
   implement stage by stage — a real `SocketChannelMiddleware` (second `ChannelMiddleware` impl,
   alongside `PassThruMiddleware`), a 3-node real-socket `ClusterKit` harness in test scope, and
   integration tests mirroring the behaviors currently covered only by PassThru-mocked tests
   (election, put/get/delete/expire, partition-restore, hard-stop/restart). **Snapshots/log
   compaction are explicitly pushed later** — the user: "we'll do snapshots later though so don't
   forget about them". `SNAPSHOTS.md` stays the authoritative backlog for that work (Follow-up #5);
   nothing in `NETWORKING.md` implements snapshot transfer — it only records that the wire format
   is payload-neutral for it. **Resolution (2026-11-21):** all four stages delivered — commits `6dc8a1e`, `7a7eb1f`, `b54199b`, `f9b2fde` — all 7 IT scenarios green over real sockets (**267 tests green, 0 failed, 1 pre-existing skip**, `/tmp/opencode/mvn-net-14.log`); the snapshots deferral stands verbatim and `SNAPSHOTS.md` remains the backlog (Follow-up #5). **Addendum (2026-11-23):** the deferral was lifted — the user signed off all snapshot stages and they were implemented in full (stages 1→6(b), commits `47d9f9f`…`stage 6(b)`, see Decisions #2 and Follow-up #5), including `InstallSnapshot`/`Response` over the wire in the payload-neutral format of this design.
9. **Wire serialization: JDK `java.io.Serializable`, no new dependencies** *(decided with #8)*.
   Offline Maven forbids adding a codec library (nothing beyond the current pom is available), and
   `jackson-databind` in the local repo is not a declared dependency — introducing it is still a
   new dependency. The wire surface is small and closed (11 message types + the 3 `KeyData*Input`
   payload types + 5 value types), and `KeyDataInput` internally holds a `Map.copyOf(...)`
   (JDK `ImmutableCollections` map) whose external form under JDK serialization is an opaque
   blob that round-trips byte-exact within one code version — adequate for in-JVM ITs and
   recorded honestly as not cross-version-safe (a hand-rolled codec, already designed as the
   escape valve in `NETWORKING.md` §2.1, is the step if cross-version compatibility is ever
   required). Frame format (4-byte BE length + payload) is payload-neutral, so that swap does not
   re-touch framing.

## Follow-ups / backlog

Everything that must not be silently dropped. "Deferred" = user-accepted for now, but must
exist before this is a production cache. Cross-references point at the Decisions above,
which stay the detailed record.

1. **DELETE over Raft** *(done in this branch — Signed-off work plan item 1, commit `40700d0`)*
   — Decisions #5. A `KeyDataDeleteInput` entry, a bridge apply path via `PartitionMap.deleteData`
   (absent-key no-op ⇒ idempotent under Raft), and `DistributedCache.delete(key)` routed through
   the leader; its future completes only after commit **and** apply, so a read immediately after
   sees the key absent (no polling). Bridge-level tests in `StateMachineBridgeTest` (apply,
   absent-key no-op, put/delete/put idempotency under re-delivery) plus DELETE e2e in
   `DistributedCacheTest` (3-node and single-node: put → delete → absent, no-op delete still
   completes, re-put visible with the correct version counter). Closes Decision #5.
2. **Spill data out of memory to a consistent (durable) store** *(deferred by the user —
   "skip it for now, it's for later"; not started)* — README promises "expire data out of the map
   to a consistent store (synchronously)". **Interpretation clarified by the user**: this means
   moving data *out of memory onto disk* / a durable consistent store (per-entry TTL is a
   separate item, see #3). Nothing exists: no spill/eviction path, no disk sink, no
   consistency guarantee to the store.
3. **EXPIRE / TTL (per-entry expiration)** *(done in this branch — Signed-off work plan item 2,
   commit `f8e901d`)* — TTL as its own capability: per-entry TTLs on values, an expiration
   mechanism (sweep, checked read, and a replicated `EXPIRE` command), consistent across the
   cluster. Delivered: `KeyDataExpireInput(key, ttl)` routed through the leader like SET/DELETE;
   expiration resolved at **apply time** against an injectable `Clock` held by `PartitionMap`
   (default `Clock.systemUTC()`, test 3-arg constructor — the 2-arg one is preserved) and stored
   per entry as an absolute deadline in `DataVersion` (`NO_EXPIRY = Long.MAX_VALUE`);
   `getData` serves expired entries as absent (checked read); `sweepExpired()` frees memory and
   emits one `DeleteDataEvent` per removal; a late EXPIRE never resurrects an already-expired key;
   EXPIRE on an absent key is a no-op (idempotent under Raft). `DistributedCache.expire(key, ttl)`
   facade. Recorded choice: each node resolves "now" from its own clock within milliseconds —
   acceptable for a cache, and avoids trusting the leader's clock on replay. Guarded by
   clock-deterministic bridge tests (deadline boundary, absent-key no-op, no late resurrection,
   second-EXPIRE-extends), map-level sweep/event tests, input validation, and e2e EXPIRE on the
   3-node and single-node clusters sharing one `MutableClock`.
4. **Fixed cluster topology** *(user-deferred)* — membership is fixed at startup; changing it
   today means taking the cluster down and rolling it over. No join/leave protocol, no joint
   consensus, no member transitions. (`disconnectFrom`, Decision #3, has since been implemented
   as a link-level operation — it tears down individual live links, which composes with a fixed
   topology — but consensus-driven join/leave remains deferred under this entry.)
5. **Snapshots / log compaction** — **done 2026-11-23** *(investigated 2026-11-21; item 4 first delivered as the design document `SNAPSHOTS.md`, then all implementation stages landed 2026-11-23, commits `47d9f9f` → `stage 6(b)`)* — Decisions #2, design in `SNAPSHOTS.md`. Ground truth: committed entries are **never** compacted (only the uncommitted tail is trimmed, `RaftLogs.rollback()`), no snapshot message / follower install / leader trigger exists, and `PartitionMap.getSnapshot()` — the exact state a snapshot must capture — is already implemented but never called. Remaining work is the staged plan (`SNAPSHOTS.md` §4): Stage 1 state capture + `replaceState`/`adopt`; Stage 2 `RaftLogs` snapshot boundary + `compact()` (closing the `containsStartPoint` TODOs); Stage 3 `InstallSnapshot`/`Response` messages (sketch-shaped: term, leaderId, lastIncluded index/term, payload, offset/done); Stage 4 follower install behind the S2 progress-only guard; Stage 5 leader laggard detection + send + compaction trigger behind the S1 bound. Stages 1–2 are isolated and testable without protocol risk. `InstallSnapshotExample` / `proto/raft/*` remain the working notes (do not delete, see Decisions #2). The staged plan has since been implemented in full (stages 1→6(b), 2026-11-23, commits `47d9f9f` → `stage 6(b)`; per-stage evidence in `SNAPSHOTS.md`'s header table and §7; final suite 320 green, 0 failed, 1 pre-existing skip) — this follow-up is closed.
6. **Election up-to-date rule** *(done in this branch)* — Decisions #1. The candidate's
   `VoteRequest` now reports the last log entry's term/index per Raft §5.4.1 (commit
   `6b33a9c`), with the voting path fixed so the rule is honored (`2c4dd89`): no rollback on
   the vote step-down paths, granted-only vote tally, and per-node `interrupt`/`restore`.
   Pinned by `testUpToDateRuleStaleCandidateLoses` (voter side) and discriminable by
   `testUpToDateRuleUncommittedTailLoses` — the 7-node liveness proof that the old
   committed-based report starves the split while the new last-entry report elects the
   fresh node (commits `2c4dd89`, `1dc88a6`).
7. **Test-hardening: at most one leader per term (item #4 from the hardening list)** *(done
   in this pass)* — the harness exposed only the max-term leader
   (`getRaftLeader` = `.filter(LEADER).max(term)`), so two simultaneous LEADER-state nodes at
   the *same* term were unexpressible, and the poll windows accepted any "good" snapshot,
   hiding transient split-brain. Fix: `ClusterNodes` exposes **every** LEADER-state node
   (`leaders()`), and the at-most-one-leader-per-term invariant is asserted on **every** poll
   sample (not just accepted ones) in the split/restore windows of
   `testElectionWithFailure_oneNode`, `testElectionWithFailure_twoNodes`, and
   `testLogReplicationDuringTermChange`. A leader that is partitioned off and holding an
   **older** term is legal; a second leader at the *same* term is not, even transiently.
8. **Test-hardening: old-leader step-down (item #3 from the hardening list)** *(done in this
   pass)* — the failure tests interrupted the old leader, restored the network, and then
   only asserted `leader != null && followers == 2` — they never proved the old leader stepped
   down, so a stale leader that never recovers from its belief would still pass. Fix: capture
   the pre-failure leader; after restore, assert it is no longer LEADER at an overtaken term —
   stepping down to follower is fine, and winning a fresh election at a *higher* term (becoming
   the current leader again) is fine; staying LEADER while another node holds leadership is not.
9. **Carried from Decisions** — the phase 3 network
   transport. (`disconnectFrom`, Decision #3, resolved 2026-11-24 as infra-debt item 4:
   `RaftNode#disconnectFrom` + `ChannelSide#close` on both middlewares, pinned by the 2-node
   quorum test and exercised over the real wire in `RaftNodeDisconnectTest`; `RaftLogs`
   performance, Decision #6, resolved 2026-11-24 with the O(1)
   tail-append fast path; `Replica` stub, Decision #4, resolved the same day by removing it
   as dead code; `Main.java` cluster demo, Decision #7, resolved the same day by rewiring the
   demo as a 3-node loopback socket cluster driving the cache facade.)
10. **Pre-Vote (optional Raft election extension)** — ~~explicitly deferred by the user on
    2026-11-20 ("Reframe now + Pre-Vote later")~~ **implemented 2026-11-21, commit
    `f35e60d` (242 tests green):** a follower's election timeout now runs a hypothetical
    pre-vote round for `term+1` before touching its term; it converts to candidate only on a
    pre-vote majority (own grant counted, etcd-style); responses never advance the receiver's
    term; a live leader's `AppendEntries` invalidates an in-flight round. Partitioned nodes
    stay FOLLOWER retrying pre-vote rounds instead of burning real terms — the term churn
    this entry warned about. `RaftNodeTest.testElectionWithFailure_twoNodes` updated to
    assert the surviving invariant (one leader, isolated pair are non-leaders) under both
    world models; new `RaftNodeBehaviorPreVoteTest` pins the 10 deterministic grant/deny
    contracts. The deferral's motivation, still accurate below —
    catch-up *while a member is still degraded* (e.g. ≥ 200 ms per-hop latency) is **not** a basic-algorithm guarantee:
    an isolated/rejoined server disrupts the cluster (term churn) — Ongaro's thesis §9.6
    ("Preventing disruptions when a server rejoins the cluster") documents this and prescribes
    **Pre-Vote** (a candidate wins *hypothetical* votes from a majority before it is allowed
    to increment its term); the paper concedes the same for removed servers in §6 ("resulting
    in poor availability"). Production etcd implements Pre-Vote and default-enables it from
    v3.5. All citations are verbatim-verified in `raft-notes.md` (paper §5.2 / §5.4.1 / §6,
    thesis §9.6 full text, etcd `raft.go` / `config.go` / `--pre-vote` docs). The implementation
    (now done, item 3 above) touched the election core in `src/main` (new pre-vote message(s) +
    candidate and follower logic) as this entry anticipated; with it in place,
    `RaftNodeFlakySlowNodeTest` gains the ability to additionally pin catch-up *during* the
    degradation window. The baseline implementation was spec-compliant without pre-vote — it
    is an availability improvement, not a correctness fix.

11. **Cross-language wire codec — migrate off JDK `java.io.Serializable`** *(deferred; trigger:
    a first client in a non-Java language (e.g. Node.js), or recorded as B's §3.1 message contract
    is being written)* — Decision #9 chose JDK serialization on the offline-constraint basis
    (nothing beyond the current pom in the local repo) and recorded it honestly as
    not cross-version-safe, with a hand-rolled codec as the escape valve. For a non-JVM client
    that basis does not transfer at all: Java serialization binds to Java classes and
    `serialVersionUID` values and leaks JDK-internal structure (a `KeyDataInput` entry's
    `Map.copyOf(...)` map round-trips only as an opaque blob — Decision #9's own wording).
    **Chosen direction (2026-11-24, user discussion; new-dependency gate cleared the same day):
    Protocol Buffers as the wire codec, over the existing TCP transport.** Rationale: (a) one `.proto` IDL becomes the shared
    contract between Java and any client language — Node's `protobufjs` loads the same file at
    runtime, so the client side has no build step; (b) protobuf's field-number schema evolution
    (unknown fields are skipped on read) is exactly the cross-version / rolling-upgrade safety
    Decision #9 records JDK serialization as lacking; (c) a top-level `oneof` envelope would
    subsume the `instanceof` dispatch chain (`NodeConnectionOutboundImpl`, in-code `// TODO:
    not very maintainable`), so the codec swap and the dispatch cleanup become one change;
    (d) it resolves both existing serialization debts — the non-`Serializable` `KeyDataOutput`
    (`design/REMOTE-CLIENT-B-CLIENT-RPC.md` §3.2) and the opaque `KeyDataInput` map blob
    (Decision #9) — by making them explicit fields. **Considered and not chosen:** gRPC (same
    `.proto`, but swaps our socket/handshake/interrupt–restore layer for library-managed HTTP/2 —
    a much larger rewrite at the PoC stage, buying no feature we currently need; it stays
    on record as a later optional upgrade that keeps the IDL); FlatBuffers / Avro / Cap'n Proto
    (no first-class language-neutral story, or zero-copy we do not need at this message size);
    msgpack (compact but no standard IDL or evolution story); and **JSON via `jackson-databind`
    — the only cross-platform option feasible today under the offline constraint** (it is already
    in the local repo, managed by the `spring-boot-starter-parent` BOM): Node-native, human-
    readable on the wire, zero new downloads, but no IDL and no typed evolution.
    **Feasibility gate (verified 2026-11-24 — CLEARED):** the offline local Maven repo contains
    `jackson-databind` but **not** `protobuf-java`/`protoc`, so adopting protobuf means adding a
    new dependency — and the user approved the one-off exception on 2026-11-24 ("You can add a new
    dependency? I don't care"). Mechanics verified the same day: this environment reaches Maven
    Central (artifact fetch OK), so `protobuf-java` downloads normally at implementation time —
    no vendoring step. The jackson JSON path survives only as a fallback if a future build is
    made hermetic without network access. The `.proto` IDL is authored first as the contract
    document either way (authoring an IDL costs no dependency), which is what keeps the eventual
    swap field-for-field.
    **Sequencing vs B is orthogonal:** B's new message set (`design/REMOTE-CLIENT-B-CLIENT-RPC.md`
    §3.1 — four requests + two responses) is small and cheap to port in either order; B's required
    constraint either way is that its payloads are flat, language-portable values
    (string / byte[] / long) and that its contracts be expressible in the IDL — both hold under
    any codec. **Open sub-questions, deliberately undecided:** build mechanics
    (`protobuf-maven-plugin` + protoc artifact vs. a reflection / dynamic-message runtime — both
    keep the `.proto` as source of truth); the `protobuf-java` version (pick latest stable at
    implementation time); whether `Handshake` negotiates the codec (the natural
    rolling-upgrade seam — a node speaking both during the migration); and whether B lands on JDK
    serialization and the six messages are ported later, or B lands after the codec swap.

## Test evidence

Counts are `mvn -B test` totals over the whole module. `RaftNodeTest` is parameterized (5
iterations per `@ParameterizedTest`), so the test total moves with it.

- Baseline (branch start, before any change): **152 tests green**, incl. 130 `RaftNodeTest`
  iterations.
- After the apply loop + hardening (commits `9a15525`, `eece74f`): **163 tests green**, adding
  `StateMachineBridgeTest` (6) and `RaftNodeTest.testStateMachineApply` (5 iterations).
- After the behavior fixes (commit `c8f5b0b`): **173 tests green**, adding two `RaftNodeTest`
  regression tests × 5 iterations:
  - `testSingleNodeElectionCommitApply` — a 1-node cluster self-elects a leader, commits a
    `KeyDataInput` by its own log alone, and the bridge applies it: `submit().get()` returns,
    `bridge.getLastAppliedIndex()` ≥ the committed index, `PartitionMap` holds the value, and the
    `DataVersion` is exactly 1 (applied exactly once, no duplicate application).
  - `testTwoNodeCommitApply` — a 2-node cluster elects a leader, replicates the entry to the
    follower, and commits it with leader + follower = the quorum of 2; same apply/version
    assertions, plus the follower's log caught up to the committed index.
- After the phase-4 client facade (commit `a9b103e`): **184 tests green**, adding
  `DistributedCacheTest` — 11 tests: `put`/`get` through a 3-node cluster (×5), `put`/`get`
  through a single-node cluster (×5), and the constructor guard. `DistributedCacheTest` is the
  regression guard for the client-facing API: leader routing, plus the read-your-writes ordering
  that lets `get` see the value the moment `put(...).get()` returns (no timeout polls).
- After the test-hardening pass (commits `01341d8`, `0536a86`): still
  **184 tests green** (`mvn -B test`), same 145 `RaftNodeTest` iterations — the test *set*
  did not grow, but the existing failure/term-churn tests now prove two guarantees they
  previously could not:
  - **At most one leader per term** — `testElectionWithFailure_oneNode`,
    `testElectionWithFailure_twoNodes`, and `testLogReplicationDuringTermChange` now assert,
    on **every poll sample** (~50 ms) of every split/restore window, that no term is ever in
    LEADER state on two nodes. `ClusterNodes` exposes `leaders()` (all LEADER-state nodes);
    before, `getRaftLeader` returned only the max-term one, which made a second simultaneous
    leader invisible. A partitioned old leader still holding an **older** term remains legal;
    a violation fails on the sample that catches it instead of expiring quietly into the
    5-second poll timeout.
  - **Old-leader step-down** — the same three tests capture the pre-failure leader and
    assert that after the restore it is no longer LEADER at an overtaken term: stepping down
    to follower is the pass; winning a fresh election at a higher term (becoming the current
    leader again) is also the pass. A leader that never relents could previously pass these
    tests, or only fail via the 5-second poll timeout with no message naming the cause.
- After the up-to-date-rule fix (commits `6b33a9c`, `2c4dd89`, `1dc88a6`): the suite grows
  184 → 189 → 194. The candidate's `VoteRequest` now reports the last log entry's term/index
  (Raft §5.4.1); the regression pin exposed two voting-path defects fixed the same commit
  (`2c4dd89`), and a follow-up added the liveness discriminator:
  - `testUpToDateRuleStaleCandidateLoses` (commit `2c4dd89`, **189 tests green**) — 3 nodes;
    one stale follower is isolated and lags, then the split leaves {stale, fresh} live; the
    stale candidate is refused by the up-to-date rule (Raft §5.4.2) and a fresh follower
    wins, with `assertAtMostOneLeaderPerTerm` on every poll phase. Honest framing: in this
    test committed == last for every voter, so old and new rules decide every vote
    identically — it is NOT a discriminator of `6b33a9c`; it pins the observable rule. The
    same commit fixed the two defects the pin exposed: the vote step-downs were rolling back
    uncommitted tails *before* the vote decision ran (`convertToFollowerForVote` now omits
    the rollback at all `onVoteRequest` step-down sites; the explicit step-down paths keep it),
    and `onVoteResponse` counted ungranted responses toward the majority (granted-only now).
    `PassThruMiddleware.interrupt`/`restore` also became per-node, so restoring a partition
    can no longer unseal the other side of a paired channel.
  - `testUpToDateRuleUncommittedTailLoses` (commit `1dc88a6`, **194 tests green**) — the
    LIVENESS DISCRIMINATOR of `6b33a9c`: 7 nodes, quorum 4; the two fresh followers hold an
    *uncommitted* tail (leader + 2 = 3 < 4, so it can never commit), the two stale followers
    lack it, and the live partition {A, B, C, D} is exactly quorum. New rule: the fresh
    candidate's last-entry report gets the tie from the other fresh node plus both stale
    grants → 4 of 7 → the fresh node wins. Old rule (committed report): the fresh candidates
    cap at 3 of 7 (their own fresh peer refuses `(T, 1)` vs own `(T, 2)`), stale candidates
    cap at 2 → no candidate can ever reach quorum → the fresh-wins assertion times out.
    Discrimination was PROVEN, not just argued: with the committed-based report temporarily
    restored the test fails 5 of 5 with the predicted starvation (the log shows the fresh
    pair denying `(1, 1)` vs `(1, 2)` across terms 19..27, no election — deterministic); with
    the fix in place it passes 5 of 5 and the log shows the tie-grant `((T, 2) vs (T, 2))`
    the old rule refused. Direction is liveness only: committed entries are a subset of last
    entries, so the old rule is strictly more conservative — it can only deny votes the new
    rule grants (starvation), never the reverse (no stale-leader safety violation exists to
    discriminate against).
- Result: **194 tests green** (`mvn -B test`), all 152 baseline tests still passing, including
  the existing `RaftNodeTest` suite at 155 iterations (130 baseline + 5 apply-loop + 10 new +
  5 `testUpToDateRuleStaleCandidateLoses` + 5 `testUpToDateRuleUncommittedTailLoses`).
  The apply-then-acknowledge ordering is what lets these assert the bridge index and map state
  *immediately* after `submit().get()` / `put(...).get()` returns, deterministically
  (no timeout polls).
- `RaftLogsTest` is the regression guard for the two-phase commit/acknowledge contract.
- `testSingleNodeElectionCommitApply` / `testTwoNodeCommitApply` are the regression guards for the
  single-node election and leader-counts-quorum behavior fixes.
- Existing `testLogReplication*` remain the replication-regression guard.
- `testUpToDateRuleStaleCandidateLoses` / `testUpToDateRuleUncommittedTailLoses` are the
  regression guards for the up-to-date election rule (`6b33a9c`) and the voting-path fixes
  (`2c4dd89`); the latter is the proven liveness discriminator of the report fix.
- After the vote-burn fix (commit `77638bd`, sign-off'd): **195 tests green** (`mvn -B test`),
  adding `RaftNodeBehaviorFollowerVoteTest` — a deterministic 3-sequence election in a single
  term: node X denies a stale candidate A (A's log behind X), then a fresher candidate B
  arrives the same term and must still be grantable. PASSES on the fix; **proven to FAIL
  against the old code** (the denial had latched `votedForNodeId`, so B got no response — an
  entire wasted term). This is the regression guard for "a vote denial must not burn the
  node's single vote".
- After the per-node fault-injection knobs (commit `209696d`): still **195 tests green**
  (`mvn -B test`) — `PassThruMiddleware` gains `setNodeLatency` / `setNodePacketLoss` /
  `clearNodeFaults` / `clearAllNodeFaults` keyed by `NodeAddress`, additive on top of the
  existing 1–9 ms baseline (reordering preserved), with `ConcurrentHashMap` backing and
  boundary/exception contract. Knobs unused ⇒ behavior identical to the pre-change network,
  so no existing test was disturbed.
- After the slow-follower / flaky-node test reframe (commit `37d93f5`): **200 tests green**
  (`mvn -o -B test`, twice in a row: 200 run, 0 failed, 1 skipped — the pre-existing
  `@Disabled` in the untouched `proto/raft` package), adding `RaftNodeFlakySlowNodeTest`
  (5 tests) on top of the new fault knobs:
  - `testSlowFollower_clusterStaysLive_andCatchesUpAfterRecovery` (renamed from
    `testSlowFollower_catchesUp_afterLeaderCommits`) — refactored to pin the contract the
    *basic* algorithm actually guarantees when one member is degraded: the healthy quorum
    (2 of 3) keeps committing a burst and no split brain forms; then `clearNodeFaults` and
    the slow node **reconverges** to the committed tail. The previous version asserted
    catch-up *while the node was still slow* — a livelock on the spec side (79/79 vote lines
    to C were `grant=false`, terms climbed to 69, C's log stuck at (0,0)). Not a spec
    deviation: Raft §5.2 mandates the term bump on candidacy, §5.4.1 mandates A/B denying C,
    and both the paper (§6: "resulting in poor availability") and Ongaro's thesis §9.6
    document exactly this disruption and the Pre-Vote extension as the fix (etcd:
    default-enabled since v3.5). All sources verbatim-verified under
    "Spec verification: slow-follower / rejoin liveness (2026-11-20)" in `raft-notes.md`
    (commit `5964f55`).
  - `testFlakyFollower_clusterStaysLive` — 70 % per-hop loss on one follower; the healthy
    quorum keeps committing regardless (the flaky node may even win an election on the good
    30 % — the test never pins the submit to a specific node).
  - `testSlowLeader_clusterStaysLive_andKeepsCommitting` — leadership may or may not move
    when the leader is slowed; both outcomes are legal and both are asserted live + safe
    (the old version pinned the submit to `oldLeader` and was the flakiness this removes).
  - `testNonLeader_submitFailsFast_notHangs` — a non-leader's `submit` rejects promptly
    (`IllegalStateException` wrapped in `ExecutionException` within 1 s).
  - `testKnobContract_boundariesAndDeterminism` — API boundaries on the new fault knobs
    (negative latency, rate < 0 / > 1, null address all rejected; fluent setters) plus the
    deterministic endpoints: loss 1.0 IS full isolation (healthy quorum commits; the isolated
    node is lagging) and `clearNodeFaults` IS restore (the isolated node catches up again).
- `RaftNodeFlakySlowNodeTest` is the regression guard for: the 3-node cluster staying live
  and safe while a single member degrades (latency OR loss), a non-leader's `submit` failing
  fast, and the fault knobs' contract. It complements the existing `RaftNodeTest`
  election/replication hardening (at-most-one-leader-per-term, old-leader step-down) by
  proving the same invariants under a **degraded but not cut** link — a distinct failure
  mode from `interrupt`/`restore` (binary isolation), which the new knobs model
  probabilistically and as a latency floor.
- After Signed-off work plan item 1, DELETE over Raft (commit `40700d0`): **213 tests green**
  (`mvn -o -B test`, green 2×; log `/tmp/opencode/mvn-delete.log`), adding the 13 new DELETE
  tests: 3 bridge-level cases in `StateMachineBridgeTest` (apply, absent-key no-op,
  put/delete/put idempotency under re-delivery) and 10 DELETE e2e iterations in
  `DistributedCacheTest` (`testPutDeleteReputThroughCluster` ×5 on the 3-node cluster,
  `testPutDeleteSingleNodeCluster` ×5 on the 1-node cluster). The e2e pairs pin the full path:
  leader routing, apply-then-acknowledge (absent immediately after `delete(...).get()` returns,
  no polling), the no-op delete's safe future completion, and the version counter showing
  exactly-once application in log order (v1=1 → delete=2 → no-op delete=3 → v2=4).
- After Signed-off work plan item 2, TTL/EXPIRE (commit `f8e901d`): **232 tests green, 0
  failed, 1 pre-existing skip** (`mvn -o -B test`, green 2×: `/tmp/opencode/mvn-ttl.log`,
  `/tmp/opencode/mvn-ttl-2.log`),
  adding 19 TTL tests on top of the 213:
  - `StateMachineBridgeTest` (+4, bridge level): `testExpireMakesTheKeyAbsentOnceItsTtlHasPassed`
    (read at the boundary: present at 4999 ms, absent at exactly 5000 ms; the value itself is
    unchanged by EXPIRE), `testExpireOfAbsentKeyIsANoOp` (no entry created; a later genuine put
    still applies), `testLateExpireCannotResurrectAnAlreadyExpiredKey` (a late EXPIRE after the
    deadline leaves the key absent — idempotent and safe under Raft replay), and
    `testSecondExpireExtendsTheDeadline` (a second EXPIRE pushes the deadline forward —
    present past the first deadline, absent past the second). All driven by a deterministic
    `MutableClock` (starts at `Instant.EPOCH`, advanced explicitly — no wall-clock sleeping),
    which is the new shared test helper `src/test/.../lib/MutableClock.java`.
  - `PartitionMapTest` (+5, map level, no Raft): sweep removes exactly the expired entries and
    emits one `DeleteDataEvent` per removal; EXPIRE of an absent key reports no change and
    creates nothing; non-expired and TTL-less entries survive the sweep; a late EXPIRE never
    resurrects an expired entry; `KeyDataExpireInput` rejects null key/ttl (NPE) and
    non-positive TTLs (IllegalArgumentException).
  - `DistributedCacheTest` (+10, e2e): `testExpireThroughCluster` ×5 (put → `expire(2s)` →
    present at 0 ms, present at 1500 ms, absent at 2100 ms on a cluster whose `PartitionMap`
    is wired to one shared `MutableClock`; plus the facade no-op path for an absent key) and
    `testExpireSingleNodeCluster` ×5 (the same on the single-node cluster). The `expire()`
    facade's future completes only after the entry is committed AND applied (apply-then-
    acknowledge), which is what lets the test read `present` immediately after
    `expire(...).get()` returns, deterministically.
- After Signed-off work plan item 3, Pre-Vote (commit `f35e60d`): **242 tests green, 0
  failed, 1 pre-existing skip** (`mvn -o -B test`, green 2×: `/tmp/opencode/mvn-prevote.log`,
  `/tmp/opencode/mvn-prevote-2.log`) — the +10 are the new `RaftNodeBehaviorPreVoteTest`
  (deterministic, inside the pre-vote election-timeout window):
  - **Follower voter** — grants a same-or-higher-term candidate with an up-to-date log and
    denies a same-term candidate whose log is behind (`(0,0)` vs own `(1,5)`); a
    lower-term request is denied. The grant is a pure prediction: the node remains a
    FOLLOWER, its term and log are untouched, and **no `VoteResponse` is ever emitted**
    (a follower that would burn a vote answering a pre-vote would break the later real
    election it promised).
  - **Candidate voter** — grants ONLY a strictly higher-term candidate whose log is
    up-to-date (a same-term request is denied even with an up-to-date log — the candidate
    has already voted for itself that term); a response still never changes
    `term`/`votedForNodeId`/`state`.
  - **Leader voter** — grants a strictly higher-term candidate with an up-to-date log,
    denies a same-term one; the node stays LEADER at its term — fire-and-forget.
  - **Collector (follower)** — converts to CANDIDATE only after its own grant plus peer
    grants reach a pre-vote majority; a round that never reaches majority leaves the node
    a FOLLOWER at its original term; foreign-term responses are ignored (a round for
    term N is deaf to term N+1 chatter).
  - **Round invalidation** — a live leader's `AppendEntries` arriving mid-round invalidates
    the in-flight pre-vote (the follower drops its own round before evaluating the
    message) — the etcd rule that a live, reachable leader means "there is already a
    leader; don't fight for the throne".
  One existing expectation needed updating to the new protocol:
  `RaftNodeTest.testElectionWithFailure_twoNodes` phase 2 previously asserted the two
  isolated nodes **self-promoted to CANDIDATE** (the old rule: every election timeout
  became a candidacy; partitioned, each re-elected at higher terms — the exact churn this
  item removes). Now an isolated follower holds only its own pre-vote grant (1 <
  majority(3)=2), so it stays FOLLOWER retrying pre-vote rounds and can never elect
  itself either (1 < 2 real votes). The assertion was tightened to the invariant that
  survives BOTH world models: exactly one leader, and the isolated pair are non-leaders
  (followers under pre-vote, candidates under the old rule); `assertAtMostOneLeaderPerTerm`
  still runs on every poll sample. The rest of the suite already passed untouched — this
  was the only observation of the old promotion behavior.
- After Signed-off work plan item 4, Snapshots / log compaction (investigation + design only, no code in this pass): **no source-file changes** — the deliverable is `SNAPSHOTS.md` (ground truth, Raft §7 gap analysis, invariants S1–S5, staged implementation plan), so the suite state is inherited from item 3: **242 tests green, 0 failed, 1 pre-existing skip** (`mvn -o -B test`, green 2×: `/tmp/opencode/mvn-prevote.log`, `/tmp/opencode/mvn-prevote-2.log`). Per the signed-off plan clause ("A partial implementation only if the full suite stays green; otherwise the design deliverable stands"), the design deliverable stands and the staged plan is the Follow-up #5 implementation record.
- After Phase 3 scoping (networking design, no code in this pass): **no source-file changes** — the deliverable is `NETWORKING.md` (wire format: 4-byte BE length + JDK `Serializable` payload; the second `ChannelMiddleware` impl `SocketChannelMiddleware` with PassThru-parity `interrupt`/`restore`; take-down/restart semantics; invariants N1–N6; the 7 IT scenarios and the staged A→D plan), plus the Decisions #8 (snapshots explicitly deferred — "we'll do snapshots later", `SNAPSHOTS.md` stays the backlog) and #9 (serialization: JDK `Serializable`, zero new dependencies, hand-rolled codec as recorded escape valve). Suite state inherited from item 4: **242 tests green, 0 failed, 1 pre-existing skip**; baseline re-verified at commit time. Implementation stages A→D each re-run `mvn -o -B test` (green before commit, logs at `/tmp/opencode/mvn-net-*.log`) before being committed.
- After Phase 3 **stage A**, wire types `Serializable` (commit `6dc8a1e`): **254 tests green, 0 failed, 1 pre-existing skip** (`mvn -o -B test`, log `/tmp/opencode/mvn-net-5.log`), adding `WireSerializationTest` (12) — the closed wire surface (11 message types + the 3 `KeyData*Input` payload types + `NodeAddress`/`NodeIdentifier`/`TermIndex`) implements `java.io.Serializable` with **zero new dependencies** (Decision #9: offline Maven forbids a codec library), and the tests pin round-trip fidelity through `ByteArrayOutputStream`/`ObjectInputStream` plus the null/boundary construction rejection each type keeps.
- After Phase 3 **stage B**, `SocketChannelMiddleware` over real sockets (commit `7a7eb1f`): **260 tests green, 0 failed, 1 pre-existing skip** (log `/tmp/opencode/mvn-net-11.log`), adding `SocketChannelMiddlewareTest` (6) — the second, *real* `ChannelMiddleware` implementation: `127.0.0.1`/ephemeral-port channels, 4-byte-BE length framing (payload-neutral for the `Serializable` payloads, so Decision #9's codec-swap escape valve does not re-touch framing), a server-side handshake that resolves the peer's `RaftNode` from its announced address, and PassThru-parity per-node `interrupt`/`restore` (invariants N1–N3: framing integrity, handshake resolution, fault isolation — sealed pairs stay framed and never desynchronize).
- After Phase 3 **stage C**, `ClusterKit` + ITs 1–6 (commit `b54199b`): **266 tests green, 0 failed, 1 pre-existing skip** (log `/tmp/opencode/mvn-net-13.log`), adding `RealNetworkClusterTest` ITs 1–6 — a production-shaped 3-node cluster (`ClusterKit` gives each node its own middleware on an ephemeral port, own `RaftManager`/`EventBus`, own `PartitionMap`/`DistributedCache`, torn down deterministically): leader election, put/get with apply-then-acknowledge visibility via the facade, delete/expire e2e, and partition-restore of a follower (N4 — the healthy pair keeps committing on a 2-of-3 quorum while the dead connection dead-marks and drops) and of the leader (N5 — the surviving pair elects a new leader at a strictly higher term; after restore the old leader steps down or re-wins at a higher term, and the at-most-one-leader-per-term rule is re-asserted on every poll sample). The `RaftNodeTest` behaviors are thereby re-proven over a real wire.
- After Phase 3 **stage D**, IT 7 hard-stop/restart (commit `f9b2fde`): **267 tests green, 0 failed, 1 pre-existing skip** (log `/tmp/opencode/mvn-net-14.log`) — the last of `NETWORKING.md`'s 7 scenarios (N6). `ClusterKit.hardStop(i)` stops a slot in crash order (facade unsubscribes → terminal `RaftManager` node stop → idempotent socket close; unlike `interrupt`/`restore` there is nothing to resume), and `restart(i)` brings the *same* slot back with process-restart semantics: a fresh `SocketChannelMiddleware` (new ephemeral wire address), a fresh discovery seeded with the two survivors' current addresses, a fresh `RaftManager` (fresh bus), a fresh node with the same Raft id, started before the slot swap; the survivors keep their bus and state machine, their facades rebuilt over a fresh node list (the old list references the terminal node, whose shut-down executor would reject a leader-routed call). IT 7 then verifies: v1 committed 3/3 and applied on all three state machines → hard-stop a **follower** (no term churn — it is never the leader) → v2 commits on a 2-of-3 quorum on the surviving pair → the stopped node's local view still reads "v1" (frozen state machine behind an unsubscribe-only `close()`) → `restart` → the surviving leader's continuation push replays v1-then-v2 into the fresh node's brand-new map (fresh `globalVersion`, so the two entries re-derive versions 1 and 2, matching the survivors byte-for-byte) → all three local maps hold "v2" at version 2 exactly → exactly one leader, the same pre-stop leader object. **Phase 3 is thereby complete** — the header above is flipped and the Phase 4 note that real transport "follows with phase 3" is delivered by these stages.
- Item 4 — snapshots / log compaction, signed-off work plan (stages 1→6(b) implemented and committed 2026-11-23 after the user signed off the full implementation; each stage ran the full suite green before commit, final log `/tmp/opencode/mvn-phase4-final.log`): **stage 1** (commit `47d9f9f`) state capture & wholesale replacement — `Snapshot` capture/restore + `StateMachineBridge` adopt; additions in `StateMachineBridgeTest`; **stage 2** (commit `a96abc8`) log boundary + `compact()` in `RaftLogs`; additions in `RaftLogsTest`; **stage 3** (commit `e9102f1`) `InstallSnapshot`/`InstallSnapshotResponse` messages + wire plumbing; additions in `WireSerializationTest`; **stage 4** (commit `d658475`) follower install acceptance + bridge adoption behind the S2 progress-only guard — `RaftNodeBehaviorFollowerInstallSnapshotTest` (16 tests); **stage 5** (commits `3fd7e6c`, `9f0a705`) leader laggard `SnapshotTransfer` with pinned serialization + compaction trigger behind the S1 bound + per-node snapshot-supplier addendum — `RaftNodeBehaviorLeaderInstallSnapshotTest` (8 tests) and the 3-node end-to-end `RaftNodeSnapshotRecoveryTest` (1 test); **stage 6(a)** (commit `35f4ec0`) chunked transfer with offset-cursor resume; additions in `WireSerializationTest`; **stage 6(b)** (commit `stage 6(b)`) durable `SnapshotStore` with save-before-trim and startup seeding — `InMemorySnapshotStoreTest` (7 tests), `FileSnapshotStoreTest` (7 tests, including empty/corrupt-store load skips). Suite at close: **320 tests green, 0 failed, 1 pre-existing skip** (up from 267 at the end of phase 3). All four signed-off work-plan items are now done.

