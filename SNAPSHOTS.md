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
| 6(c) | non-happy-path hardening: failing stores on both roles (leader compaction and follower install fail closed — log left untrimmed, no success reply so the leader retransmits), gap one below the trigger stays on the append-replay path, an exact-divisor chunk size ends cleanly on the last byte with no empty trailer, orphan `.tmp` files are invisible to `loadLatest` even when named for a higher boundary | `stage 6(c)` |
| 6(d) | non-happy-path hardening (second pass): a throwing snapshot supplier or a payload that cannot be serialized in the laggard path falls back to appends instead of tearing down the heartbeat round (a healthy supplier recovers); a negative offset cursor in `InstallSnapshotResponse` is clamped to 0 (would otherwise AIOOBE the next `Arrays.copyOfRange`); `loadLatest` skips snapshot-shaped files with unparseable names while still recovering a valid one in the same directory, and picks strictly max-by-boundary (term-major) when two valid files coexist; the compaction gate stays fail-closed; a failed store `save` (e.g. read-only directory) is loud and leaves no `.tmp` behind | `stage 6(d)` |
| 6(e) | non-happy-path hardening (third pass): the follower's `onInstallSnapshot` refuses a **negative `offset` cursor** (a byte position is never < 0) by tearing down the in-progress transfer and answering with the restart-at-byte-0 cursor — previously it would fall into the "duplicate" branch and re-advertise the stale cursor; an **explicit null payload** is refused like a non-`byte[]` one (tear-down + cursor-0 refusal, election timer rescheduled) — previously it NPEd at `chunk.length` and escaped the handler **with no reply at all** (unreachable via the public API: the `InstallSnapshot` constructor already null-guards the payload, a boundary pin now asserts that invariant); the leader's `onInstallSnapshotResponse` needed **no** change — the two untested edges of its completion predicate are pinned instead: a same-term reply that arrives with **no transfer in flight** (late/duplicated/mis-routed) completes cleanly (tail adopted, commit advanced, and nothing fabricated to retransmit, since no capture was ever pinned), and a reply whose **cursor runs past the last byte of the pinned capture** while its index stays below the boundary completes the transfer — the next pass is a FRESH install from byte 0 of the same pinned capture, never a "resume" that would AIOOBE `Arrays.copyOfRange` | `stage 6(e)` |
| 6(f) | non-happy-path hardening (fourth pass): the other side of the leader's laggard decision — a follower **strictly below** the compacted start point receives the install even when its gap is BELOW `SNAPSHOT_TRIGGER_ENTRIES` (branch (a) has no threshold: an append replay there is a permanent empty stream, `getLogRange` serves nothing at or below the seed), a follower **AT** the boundary gets only empty heartbeats, no install (the "strictly below" edge pinned from the other side), and an `InstallSnapshotResponse` carrying a **lower** term is an ordinary completion on the index axis — leadership kept (step-down only on a *strictly higher* term, `onAcknowledgeEntries` parity), reported tail still adopted (no production change: three pins, and the pins themselves surfaced a test-hygiene gap — send-count assertions must be bounds + per-message properties, never exact counts, because the 50 ms background heartbeat timer legitimately interleaves) | `stage 6(f)` |
| 6(g) | non-happy-path hardening (fifth pass): the last unguarded escape sites behind the 6d/6e-style guards, sharing one root mechanism — `TermIndex`'s constructor throws IAE on a negative term or index, and two handlers feed wire values into it **without guarding those axes**. Leader: the completion branch of `onInstallSnapshotResponse` adopts the reply's term/index into `new TermIndex(...)` (the 6d clamp covered the cursor axis only) — and *after* `snapshotsInFlight` has already been deleted, so the IAE escaped mid-cleanup, leaving the leader half-processed; each negative axis is now clamped to the closest legal value, 0 (a negative term never exceeds ours, so step-down semantics are unchanged). Follower: the accept path builds `new TermIndex(lastIncludedTerm, lastIncludedIndex)` *after* recording the sender as leader (the index axis is structurally S2-gated — strictly beyond the tail — the term axis is not), so a same-term peer with boundary term `-1` IAEd *post-mutation*; it is now refused before ANY state change, in the exact shape of the negative-offset guard (offset-0 reply + `scheduleNextElectionTimeout`, no half-transfer left to absorb the next chunk) | `stage 6(g)` |
| 6(h) | non-happy-path hardening (sixth pass): the remaining hostile-wire-tail escapes on the replication path, the 6(g) root mechanism (`TermIndex` constructor IAE/NPE) on the append/ack surface — the follower's `onAppendEntries` feeds three of the wire axes (prev-log pointer, each entry's term/index, the leader commit index) into `new TermIndex(...)` unguarded (a negative or missing field threw mid-handler after the sender was already recorded as leader, and, for the entry/commit axes, after entries were already appended), now **refused** before any state change in the shape of the sibling refusals (false ack carrying our own unchanged tail) — no clamping here, because clamping a mid-log position or match point would manufacture a bogus append or a phantom match; the leader's `onAcknowledgeEntries` feeds the follower's reported tail into `new TermIndex(...)` in **both** branches (the failure-branch rewind, the success-branch adoption), and a negative or missing tail threw before adoption, commit alignment or the catch-up send could run, now **refused as warn + ignore** (no reply exists on the ack path; connection book-keeping, log, and leadership are byte-for-byte untouched) | `stage 6(h)` |
| 6(i) | non-happy-path hardening (seventh pass): the last two hostile-wire-tail escapes of the 6(g)/6(h) family, now on the vote path — the follower's `onVoteRequest` now **refuses before any mutation** (the term-ladder conversion still runs first, then: warn + re-armed election timer + one explicit false `VoteResponse` carrying our own committed tail, mirroring the handler's own denial shape; `leaderId` never cleared, and a denial deliberately does **not** burn `votedForNodeId`, so a legitimate same-term candidate is still granted afterward) when the request's last-log term/index is a negative or missing wire field; the base-class `isUpToDate` — the single feed-in point all three pre-vote handlers share (follower/candidate/leader) — now refuses with a plain `false` (warn, no state change) on a hostile/missing tail, sealing all three roles with no handler-side change; `isHostileWireTermIndex` lifted to `RaftNodeBehavior` as a shared static (the follower's copy deleted; the three 6(h) append-path call sites resolve via the inherited static, unchanged) | `stage 6(i)` |

Final suite at stage 6(b): **320 tests green, 0 failures, 1 pre-existing skip**
(`/tmp/opencode/mvn-phase4-final.log`); after stage 6(c): **325 tests green, 0 failures,
1 pre-existing skip**; after stage 6(d): **332 tests green, 0 failures, 1
pre-existing skip**; after stage 6(e): **336 tests green, 0 failures, 1
pre-existing skip** (baseline 332 + 4 new pins: 2 follower hardening refusals + 2 leader
stale/over-length reply pins); and after stage 6(f): **339 tests green, 0 failures, 1
pre-existing skip** (+3 leader laggard-branch pins); and after stage 6(g): **341 tests
green, 0 failures, 1 pre-existing skip** (+2: one pin per TermIndex IAE escape); and
after stage 6(h): **347 tests green, 0 failures, 1 pre-existing skip** (+6: three
follower append-path refusal pins and three leader ack-path refusal pins, closing the
append/ack surface of the 6(g) hostile-wire-tail mechanism; and after stage 6(i): **352
tests green, 0 failures, 1 pre-existing skip** (+5: two follower vote-path refusal pins
and three pre-vote role refusal pins, closing the vote surface of the hostile-wire-tail
mechanism).
`InstallSnapshotExample.java` and the `proto/raft/*`
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
5. **`InProgressSnapshot` assembly buffer is unbounded — deliberate, deferred.** The follower
   assembles a chunked capture in a `ByteArrayOutputStream` until the final chunk; nothing caps the
   total bytes (only the leader's pinned capture length bounds a *well-behaved* sender). A stuck or
   hostile leader could feed in-order, non-final chunks forever and pin unbounded heap on the
   follower — a memory-exhaustion DoS surface. Deliberately out of scope for the non-happy-path
   hardening (it is a resource-policy design call, and the honest bounds are: a max-transfer-size
   setting, or spilling the in-progress buffer to the durable `SnapshotStore` `.tmp` mechanism, or a
   transfer TTL / abort-reason in the reply). Recorded 2026-11-23 at stage 6(e) so it stays visible
   and is not silently "fixed" by a naive size check that would break large-but-legitimate captures.

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

**Non-happy-path hardening pass (2026-11-23, stage 6(c))** — the five failing/edge paths that the
happy-path suite never exercises, verified by isolated class runs first, then a green full suite:
`RaftNodeBehaviorLeaderInstallSnapshotTest` gains the gap-just-below-trigger case (a follower at
`SNAPSHOT_TRIGGER_ENTRIES − 1` stays on the append-replay path — four appends replayed starting at
its index + 1, no install, no compaction), the exact-divisor chunk edge (`SNAPSHOT_CHUNK_BYTES` set
so the capture is an even multiple of two: the final chunk lands exactly on the last byte, carries
the done flag and a real payload slice, and no zero-byte trailer is ever emitted nor a chunk after
completion — `static volatile` restored in `finally`), and the leader compaction with a throwing
store (fails closed: the exception escapes `onAcknowledgeEntries`, committed is already advanced
per the commit→save→compact order, no snapshot boundary, the five entries are still
`getLogRange`-visible — the log is never trimmed on an unpersisted capture).
`RaftNodeBehaviorFollowerInstallSnapshotTest` mirrors the follower side: the final chunk
deserializes, the store refuses to persist, and everything after the save in the S4 order must not
happen — no `InstallSnapshotResponse` (so the leader retransmits), no snapshot boundary, log and
applied index untouched, the state machine never sees the payload.
`FileSnapshotStoreTest` proves an orphan `.tmp` left by a crash mid-write is invisible to
`loadLatest` — deliberately seeded with a HIGHER boundary than the real snapshot, so a
parse-then-max-by-boundary mistake would recover it and the test fails.
Final suite: **325 tests green, 0 failures, 1 pre-existing skip** (+5 over the 6(b) baseline).

**Non-happy-path hardening pass, second round (2026-11-23, stage 6(d))** — the paths 6(c) left
open: the leader's laggard supplier path, the offset-cursor contract, and the store's loader.
Three production hardenings: `RaftNodeBehaviorLeader` wraps the laggard path's
`snapshotSupplier.get()` + `serializeSnapshotPayload` in a `RuntimeException` guard (WARN,
transfer stays null, the heartbeat round falls back to per-entry appends — the same
null-fallback precedent `onAcknowledgeEntries` already follows); `onInstallSnapshotResponse`
clamps a negative `offset` cursor to 0 before the branch logic (an unclamped negative cursor
sets `sentOffset` negative and AIOOBEs `Arrays.copyOfRange` on the next pass); and
`FileSnapshotStore.loadLatest` skips snapshot-shaped files whose names do not parse (WARN)
instead of escaping the max-by-boundary comparator, so a valid snapshot in the same directory
still recovers.
`RaftNodeBehaviorLeaderInstallSnapshotTest` gains four tests: a throwing supplier falls back
to appends, the pass survives, and a healthy supplier recovers on the next pass; a payload that
cannot be serialized (probed raw against `ObjectOutputStream` to pin the
`NotSerializableException`, since the production wrapper converts it to `IllegalStateException`)
fails the same soft way with nothing escaping; a negative cursor reply is treated as a start
over from byte 0 of the same pinned capture; and the compaction gate with a throwing supplier
stays fail-closed (exception escapes, committed already advanced, log untrimmed, store
untouched) — the 6(c) pin re-verified under the new laggard guard.
`FileSnapshotStoreTest` gains three: a stray `raft-snapshot-foo.snap` beside a valid
`raft-snapshot-5-10.snap` is skipped — and left in place — while the valid one still loads (load
is read-only); two coexisting valid files `raft-snapshot-1-5.snap` / `raft-snapshot-2-3.snap`
resolve to the highest boundary, term-major, so (2,3) wins despite its lower index; and a
`save` into a directory whose owner write bit was revoked (POSIX-permissions probe + `Assumptions`
skip for root CI boxes) is loud (the compaction gate fails closed on exactly this) and leaves no
`.snap.tmp` behind — the `finally` cleanup ran.
Final suite: **332 tests green, 0 failures, 1 pre-existing skip** (+7 over the 6(c) baseline).

**Non-happy-path hardening pass, third round (2026-11-23, stage 6(e))** — the remaining
untouched edges of the two message handlers. Two production hardenings, both on the
**follower's** `onInstallSnapshot`: an explicit **negative `offset`** is now refused (a byte
position can never be < 0) — previously, under an in-progress transfer, it would fall into
the in-order "duplicate" branch at `RaftNodeBehaviorFollower` and *re-advertise the stale
cursor* `pending.expectedOffset`, so a buggy or misbehaving peer could pin the transfer at
an old offset forever; now the transfer is torn down, the reply carries the restart-at-byte-0
cursor, and the leader re-runs the whole install from the same pinned capture. And an
**explicit null payload** is now refused exactly like a non-`byte[]` one — previously it
NPEd at `chunk.length` (the in-order append) and escaped the handler with **no reply at
all**, leaving the transfer hanging with a partial buffer and the leader resuming into
nowhere. The null path is unreachable through the public message API — the `InstallSnapshot`
constructor already null-guards the payload (a boundary pin asserts that invariant), so this
is defensive: it keeps the handler honest against a future serialization gap instead of
turning a malformed frame into a silent protocol hole. The pinned contracts that already
covered the follower's failing-store path (`finalChunkWithFailingSnapshotStore_failsClosed_
stateNotAdopted_noReplySent`) were left untouched — the save-failure→no-reply→leader-retransmits
behavior is the intended design, and a try/catch-with-reply was explored and reverted. The
**leader's** `onInstallSnapshotResponse` needed **no change**; its two untested edges of the
completion predicate were pinned instead: a stale/duplicate reply that arrives with **no
transfer in flight** (a same-term `InstallSnapshotResponse` with `offset` < capture length
cannot be "in the middle" of anything, so it is treated as completion: the reported tail —
already past the compaction boundary — is adopted, commit advances by the majority-count-th
index, and the pass fabricates nothing to retransmit), and a reply whose cursor **runs past
the last byte of the pinned capture** while its index stays below the boundary — the leader
completes the transfer and the next pass is a **FRESH install from byte 0 of the same pinned
capture**, never a "resume" that would AIOOBE `Arrays.copyOfRange` on a slice past the
array. Isolated runs of both classes before full-suite confirmation: 17 leader + 19 follower
pins green. Final suite: **336 tests green, 0 failures, 1 pre-existing skip** (+4 over the
6(d) baseline: 2 follower pins + 2 leader pins).

**Non-happy-path hardening pass, fourth round (2026-11-23, stage 6(f))** — the other side
of the leader's laggard decision. Until this pass, only branch (b) of the two install
triggers in `RaftNodeBehaviorLeader.appendEntries` (gap ≥ `SNAPSHOT_TRIGGER_ENTRIES`) had
been exercised end-to-end with a compacted log in play; branch (a) — a follower **strictly
below** the compacted start point — and the boundary between the two branches were open on
the leader side (the follower side was fully inventoried at stage 6c/6e). Three new leader
pins, all in `RaftNodeBehaviorLeaderInstallSnapshotTest`; **no production change** — the
code under test (branch (a), the strict `<`, the `>` step-down bound) already existed from
stage 5 and was now pinned:

1. **`belowStartPoint_gapBelowTrigger_stillReceivesInstallSnapshot`** — a (re)joiner sitting
   BELOW the start point (1,6), at index 4 (gap 2, well under the threshold of 5), still
   receives `InstallSnapshot`. That is the whole point of branch (a) having **no threshold**:
   an append replay for this follower is a permanent empty stream — `getLogRange` serves
   nothing at or below the seed, the boundary record itself excluded (a pre-test assertion
   pins exactly that: `getLogRange((1,4))` is empty) — so without (a) the follower would
   stall forever on "nothing to send". First-install properties are asserted (boundary,
   offset 0, done, the pinned serialization); the send *count* is bounded, not exact,
   because the documented retransmit path may legitimately re-send the same chunk while the
   follower's cursor is still byte 0 (it never replied).
2. **`atStartPoint_gapBelowTrigger_heartbeats_notInstall`** — the "strictly below" edge,
   pinned from the other side: a follower AT the start point (index 6 = the boundary)
   receives **no install** — gap 0 fires neither branch — and only ever empty heartbeats
   (nothing remains behind the seed to replay, so the append path is quiet too).
3. **`lowerTermSnapshotResponse_keepsLeadership_adoptsTail`** — the inverse of the
   already-pinned higher-term step-down: `onInstallSnapshotResponse` steps down only on a
   *strictly higher* term (parity with `onAcknowledgeEntries`), so a reply carrying a
   **lower** term — a buggy or older peer echoing a stale term — is an ordinary completion
   on the index axis: leadership kept, our term untouched, the reported tail still adopted
   as the follower's new start point, no install fabricated, commit advanced to the
   reported index. Assertions are deliberately **index-axis only**: a term-0 reply writes
   term 0 into the follower's connection record and the commit-point term computation
   (a pre-existing quirk of the completion branch), and asserting exact `TermIndex` values
   would pin the quirk instead of the contract.

Also recorded: a **test-hygiene finding** that surfaced when these pins first went green in
isolation but red in the full suite (and vice versa under load). The leader reschedules a
50 ms background heartbeat timer (its `onHeartbeatTimeout` → `appendEntries(TIMEOUT)` pass)
on the node executor, and a tick can land mid-test, sending a legitimate extra message —
the retransmitted install, or an empty heartbeat on an INITIAL/TIMEOUT pass (a CONTINUATION
with nothing to send stays quiet). Exact send-count assertions are therefore
timing-dependent; the pins settled on the convention the earlier green pins already use:
**lower bounds + per-message properties** (every append carries no entries; the first
install has the pinned payload), never exact counts. Isolated class run: 20 leader pins
green. Final suite: **339 tests green, 0 failures, 1 pre-existing skip** (+3 over the
6(e) baseline: the three leader laggard-branch pins).

**Non-happy-path hardening pass, fifth round (2026-11-23, stage 6(g))** — the last two
unguarded entry surfaces shared one root mechanism, found by re-running the 6d/6e
question over every wire value the handlers feed into `TermIndex`: its constructor throws
`IllegalArgumentException` for a negative term or index, so any axis that reaches it unguarded
is an escape — and in both cases the escape was the *post-mutation* kind that stage 6(e)
already showed to be the dangerous one (state half-changed, no sane cleanup to fall back).

**Leader — `onInstallSnapshotResponse` (production change).** The stage 6(d) guard clamped
the reply's **cursor axis** (`offset`) only. But the completion branch also adopts the
reply's **other two axes** verbatim into `new TermIndex(responseTerm, responseIndex)` — and
that branch has *already* run `snapshotsInFlight.remove(nodeId)` by the time the
construction happens. A negative index or term in a stale reply from a misbehaving peer
therefore IAE'd mid-cleanup: the in-flight record deleted, the tail never adopted, no
acknowledgement, `updateCommittedIndex()` and `appendEntries(CONTINUATION)` never reached,
the reply never answered. The fix extends the same 6d clamp convention to the two missing
axes: each negative axis is clamped to the closest legal value, `0` (exactly as 6d reads a
negative cursor as "restart from byte 0"). Since a negative term can never legitimately
exceed the leader's own term, clamping it *before* the step-down check preserves
step-down semantics exactly. Pin
(`aNegativeTermOrIndexInAReply_isClampedToZero_andAdoptedWithoutEscaping`): a same-term
reply with index `-5` clamps to `(TERM, 0)` and is adopted exactly as a genuine tail-0
reply would be; a reply with term `-7` clamps to term 0 and — critically — does **not**
step the leader down (a negative term is not a higher term). Leadership kept, log
untouched, nothing fabricated to retransmit (no supplier registered, no transfer in
flight), and the handler's own CONTINUATION catch-up resumes the slow-appends path — the
clamped `(0,0)` tail is below every logged entry, so the entries are real. One of the pin's
own assertions had to be corrected mid-iteration, and the correction is itself a finding:
the commit point is **not** invariant under a hostile reply. A snapshot reply is itself the
acknowledgement of everything up to the reported tail (Raft §7 — the 6f pin at
`lowerTermSnapshotResponse_keepsLeadership_adoptsTail` asserts exactly this contract), so
a clamped `(TERM, 0)` tail legitimately **advances** the commit point from `(0,0)` to
`(TERM,0)`. The invariant the pin now states is the direction that must be protected: a
hostile tail can advance the commit to itself but can never move it back.

**Follower — `onInstallSnapshot` (production change).** The accept path builds
`new TermIndex(lastIncludedTerm, lastIncludedIndex)` straight from wire values, in the
chunk-cursor section. The **index axis** is structurally safe — S2 requires the boundary to
be strictly beyond our tail, so it is already `>= 0`. The **term axis** was not guarded:
a same-term peer claiming a boundary term of `-1` would IAE out of the `TermIndex`
constructor *after* the accept path had already recorded that peer as leader
(`leaderId = sender.getRemoteNodeId()`) — the same "IAE after mutation" failure class that
stage 6(e) shut down on the follower's null-payload branch, one branch further along this
very handler. The fix is a refuse-before-mutate guard in the exact shape of the existing
negative-offset guard two sections below it: `lastIncludedTerm < 0` → offset-0 reply
(`InstallSnapshotResponse(term, tail, 0)`), `scheduleNextElectionTimeout()`, warn log,
`return`. Pin (`aNegativeBoundaryTerm_isRefusedInTheSameShapeAsANegativeOffset`): a
boundary `(term=-1, index=10)` that passes S2 (index 10 strictly beyond the fresh
follower's tail, 0) is answered with the offset-0 "restart" cursor and leaves **no** trace
— tail unmoved, no start-point snapshot, and **no `leaderId` recorded** (the guard fires
before the accept path); a follow-up resend of the same "transfer" at offset 1 also bounces
to byte 0 because the refusal buffered nothing for it to land against — no half-transfer
survives to smuggle a later chunk through.

Isolated runs: leader install-snapshot class **21 green**, follower install-snapshot class
**20 green** (one new pin each). Final suite: **341 tests green, 0 failures, 1 pre-existing
skip** (+2 over the 6(f) baseline: the leader reply-axes clamp pin and the follower
negative-boundary-term refusal pin).

**Non-happy-path hardening pass, sixth round (2026-11-23, stage 6(h))** — the last two
unguarded sites of the 6(g) mechanism, on the replication path: the append and ack wire
values still feed `new TermIndex(...)` without guarding their axes, and both are the
*post-mutation* throw kind stage 6(e) showed to be the dangerous one.

**Follower — `onAppendEntries` (production change).** The accept path feeds three wire
axes into `new TermIndex(...)`: the previous-log pointer, every entry's term/index, and
the leader's commit index (the catch-all debug line builds one too). None of the axes is
validity-checked before consumption, so a hostile or corrupted append carrying one of
them would reach the throw AFTER the accept path had already recorded the sender as
leader — and, for the entry and commit axes, after some entries were already appended.
The guard (a `hasHostileWireTermIndex` check on all three axes: null, negative term, or
negative index) is placed after the term ladder (a strictly higher term still steps us
down, a strictly lower term still refuses) and before every mutation site below. The
refusal takes the exact shape of the sibling refusals in this handler: a false ack
carrying our own (unchanged) tail, zero state change — `leaderId` still null, log and
commit point at the epoch, nothing applied. A null wire field is hostile for the same
family: it NPEs out of the wire getter before the negative IAE even fires. Clamping is
deliberately **not** the tool here the way it was in 6(g): clamping an entry position
or a match point would manufacture a bogus append or a phantom match, converting a
refusal into corruption.

**Leader — `onAcknowledgeEntries` (production change).** The mirror image on the ack
path: both branches feed the follower's reported tail into `new TermIndex(...)` — the
failure branch's rewind (`setTermIndex`, its first mutation) and the success branch's
adoption (ahead of `updateCommittedIndex()`, the pacing stamps, and the
CONTINUATION catch-up send). A null or negative tail would IAE/NPE out of the handler
before any of that ran; the throw itself escaping to the caller is the hazard. The guard
sits between the step-down check (a strictly higher term still steps the leader down)
and the branches. The refusal is a warn and an ignore: there is no reply to send on the
ack path (the follower already spoke), so the contract is zero side effect — no tail
adopted, no rewind, no commit move, no pacing update, nothing sent, and no step-down
(equally, a refusal must not be misread as a higher-term claim). The next legitimate ack
or the heartbeat moves replication along.

**Pins, one per axis per branch (six total).** Follower: a negative entry index; a
negative leader-commit index (entries empty, so the commit-alignment site is the one
reached); a null entry term/index (the NPE-before-IAE family). Each keeps the
previous-log pointer valid — the epoch, which a fresh log holds — so `containsStartPoint`
passes and the guard, not another check, is what the pin proves. Leader: a negative-index
ack on the success branch, a negative-index ack on the failure branch, a null acked
tail. All assert the refusal contract: the zero-state-change shape (follower `leaderId`
still null; leader tail still `(0,0)`, commit still index 0, pacing still the epoch) and
leadership undisturbed (a refusal is not a step-down). Exact empty-send assertions are
safe on these pins in a way the 6(f) bound convention would not have allowed: the guard
fires before the send paths, and the leader pins run with no connection in
`activeConnections`, so nothing — not even the 50 ms heartbeat timer — is scheduled to
send.

Isolated runs: leader install-snapshot class **24 green**, follower install-snapshot
class **23 green** (three new pins each). Final suite: **347 tests green, 0 failures,
1 pre-existing skip** (+6 over the 6(g) baseline: three follower append-path refusals
and three leader ack-path refusals). The append/ack surface of the 6(g) mechanism is now
exhausted; the same question over the vote path still open (recorded here, not yet
pinned): `onVoteRequest` and `onPreVoteRequest` compare the last log entry's term/index
against the incoming request's term/index — two more sites where a hostile wire value
meets the same unguarded constructors and comparators, and the natural 6(i) candidate.

**Non-happy-path hardening pass, seventh round (2026-11-23, stage 6(i))** — the two
candidate sites the 6(h) pass recorded, now closed. Grep-confirmed exhaustive: these are
the only remaining places a wire `TermIndex` meets a domain constructor or comparator
without a guard.

**Follower — `onVoteRequest` (production change).** The accept path feeds the request's
last-log term/index into the up-to-date comparison *after* clearing `leaderId` — the
post-mutation throw kind the 6(g)/6(h) passes established as the dangerous one, only
here the mutation already performed is the cheaper one (no entries appended, no commit
move). The guard sits after the term ladder — a strictly higher term still runs the
`convertToFollowerForVote` re-dispatch first, so term recognition is not blocked by the
refusal — and before `leaderId` is cleared and before the comparison. The refusal takes
the exact shape of the handler's own ordinary denial below it: warn, re-armed election
timer, one explicit false `VoteResponse`. The denial carries the voter's **own**
committed tail (`self.logs.getCommittedTermIndex()`, the L159 convention): a
well-formed response even when the request's tail is hostile, and the candidate learns
our real progress instead of echoing back garbage. Crucially, the refusal leaves
`votedForNodeId` untouched — it is grant-only — so the denial does not burn the node's
single per-term vote: a legitimate up-to-date candidate in the same term is still
granted afterward. (Had the refusal burned the vote, the same-term guard at the top of
the handler would silence that candidate entirely.)

**Base class — `isUpToDate` (production change).** The single feed-in point all three
pre-vote handlers share — follower, candidate, leader — and the one place a hostile
tail met the domain constructor on the pre-vote path. The guard lives **inside**
`isUpToDate`: hostile or missing tail → warn + `false`; the three callers are sealed
with zero handler-side change. The base warn omits the remote id (no `sender` in scope
at the base); each caller already logs its own denial with context. The fit with
`isUpToDate`'s documented pure-read contract is exact: a refusal is a read result, not
a mutation, and a pre-vote denial trivially never sets `votedFor` — the pre-vote round
is hypothetical by design. As in 6(h), clamping is deliberately **not** the tool:
clamping a candidate's reported last entry would manufacture a phantom up-to-date
comparison (or a phantom log position), converting a denial into a false grant.

**Refactor.** `isHostileWireTermIndex` lifted from the follower to `RaftNodeBehavior`
as a package-private static (its parameter is the fully-qualified wire type; the base's
wildcard import makes bare `TermIndex` the domain type). The follower's private copy is
deleted; its three `hasHostileWireTermIndex` append-path call sites (added in 6(h))
resolve via the inherited static, source-unchanged — regression-verified by re-running
both 6(h) snapshot classes in the isolated run.

**Pins, five total (8 hostile-tail requests).** Two in
`RaftNodeBehaviorFollowerVoteTest`: a negative-index tail, and a null tail. Each asserts
the full refusal contract — exactly one explicit denial (not silence, not an escape),
the denial echoing the accepted term and carrying the voter's **pre-request** committed
tail, the term conversion intact (FOLLOWER of term 1, role unchanged) — plus the
discriminator: a legitimate up-to-date candidate in the same term is still granted
afterward. Three in `RaftNodeBehaviorPreVoteTest`, one per voter **role**: the follower
(term 1 request vs own term 0, `>=` gate passed in both requests), the candidate (two
peers → majority 2 blocks self-election, so a deterministic term-1 candidate; term-2
requests vs its term 1), and the leader (same fixture plus `convertToLeader()`). Each
role's voter is sent a null-tail and a negative-tail `PreVoteRequest` to two
`RecordingSenders` — exactly two denials, each echoing the request term — and then a
legitimate up-to-date pre-vote in a `RecordingSender` of its own — granted. Role and
term unchanged in all three: a refusal is not a step-down, not a promotion, not a term
advance.

Isolated runs: follower-vote class **3 green** (1 → 3), pre-vote class **13 green**
(10 → 13), both 6(h) snapshot classes unchanged at **24/23 green** — the inherited
static resolves from the follower's append-path guard. Final suite: **352 tests green, 0
failures, 1 pre-existing skip** (+5 over the 6(h) baseline). The vote surface of the
hostile-wire-tail mechanism is now closed: every site where a wire `TermIndex` meets a
domain constructor or comparator on the snapshot (6d–6g), append/ack (6h), or vote
(6i) path now refuses fail-closed before any mutation the accept path had already
performed. The non-happy-path hardening of the in-memory cache's Raft layer is
exhausted for the current surface; the next items are the planned structural backlog —
(A) spill-to-disk durable store, (B) partition ownership, (C) fixed-cluster topology,
and (D) infra debt.
