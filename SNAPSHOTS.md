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
| 6(d) | non-happy-path hardening (second pass): a throwing snapshot supplier or a payload that cannot be serialized in the laggard path falls back to appends instead of tearing down the heartbeat round (a healthy supplier recovers); a negative offset cursor in `InstallSnapshotResponse` is clamped to 0 (would otherwise AIOOBE the next `Arrays.copyOfRange`); `loadLatest` skips snapshot-shaped files with unparseable names while still recovering a valid one in the same directory, and picks strictly max-by-boundary (term-major) when two valid files coexist; the compaction gate stays fail-closed; a failed store `save` (e.g. read-only directory) is loud and leaves no `.tmp` behind (the read-only premise is platform-gated after 6(n): non-POSIX platforms skip cleanly instead of erroring on `setPosixFilePermissions`, §7) | `stage 6(d)` |
| 6(e) | non-happy-path hardening (third pass): the follower's `onInstallSnapshot` refuses a **negative `offset` cursor** (a byte position is never < 0) by tearing down the in-progress transfer and answering with the restart-at-byte-0 cursor — previously it would fall into the "duplicate" branch and re-advertise the stale cursor; an **explicit null payload** is refused like a non-`byte[]` one (tear-down + cursor-0 refusal, election timer rescheduled) — previously it NPEd at `chunk.length` and escaped the handler **with no reply at all** (unreachable via the public API: the `InstallSnapshot` constructor already null-guards the payload, a boundary pin now asserts that invariant); the leader's `onInstallSnapshotResponse` needed **no** change — the two untested edges of its completion predicate are pinned instead: a same-term reply that arrives with **no transfer in flight** (late/duplicated/mis-routed) completes cleanly (tail adopted, commit advanced, and nothing fabricated to retransmit, since no capture was ever pinned), and a reply whose **cursor runs past the last byte of the pinned capture** while its index stays below the boundary completes the transfer — the next pass is a FRESH install from byte 0 of the same pinned capture, never a "resume" that would AIOOBE `Arrays.copyOfRange` | `stage 6(e)` |
| 6(f) | non-happy-path hardening (fourth pass): the other side of the leader's laggard decision — a follower **strictly below** the compacted start point receives the install even when its gap is BELOW `SNAPSHOT_TRIGGER_ENTRIES` (branch (a) has no threshold: an append replay there is a permanent empty stream, `getLogRange` serves nothing at or below the seed), a follower **AT** the boundary gets only empty heartbeats, no install (the "strictly below" edge pinned from the other side), and an `InstallSnapshotResponse` carrying a **lower** term is an ordinary completion on the index axis — leadership kept (step-down only on a *strictly higher* term, `onAcknowledgeEntries` parity), reported tail still adopted (no production change: three pins, and the pins themselves surfaced a test-hygiene gap — send-count assertions must be bounds + per-message properties, never exact counts, because the 50 ms background heartbeat timer legitimately interleaves) | `stage 6(f)` |
| 6(g) | non-happy-path hardening (fifth pass): the last unguarded escape sites behind the 6d/6e-style guards, sharing one root mechanism — `TermIndex`'s constructor throws IAE on a negative term or index, and two handlers feed wire values into it **without guarding those axes**. Leader: the completion branch of `onInstallSnapshotResponse` adopts the reply's term/index into `new TermIndex(...)` (the 6d clamp covered the cursor axis only) — and *after* `snapshotsInFlight` has already been deleted, so the IAE escaped mid-cleanup, leaving the leader half-processed; each negative axis is now clamped to the closest legal value, 0 (a negative term never exceeds ours, so step-down semantics are unchanged). Follower: the accept path builds `new TermIndex(lastIncludedTerm, lastIncludedIndex)` *after* recording the sender as leader (the index axis is structurally S2-gated — strictly beyond the tail — the term axis is not), so a same-term peer with boundary term `-1` IAEd *post-mutation*; it is now refused before ANY state change, in the exact shape of the negative-offset guard (offset-0 reply + `scheduleNextElectionTimeout`, no half-transfer left to absorb the next chunk) | `stage 6(g)` |
| 6(h) | non-happy-path hardening (sixth pass): the remaining hostile-wire-tail escapes on the replication path, the 6(g) root mechanism (`TermIndex` constructor IAE/NPE) on the append/ack surface — the follower's `onAppendEntries` feeds three of the wire axes (prev-log pointer, each entry's term/index, the leader commit index) into `new TermIndex(...)` unguarded (a negative or missing field threw mid-handler after the sender was already recorded as leader, and, for the entry/commit axes, after entries were already appended), now **refused** before any state change in the shape of the sibling refusals (false ack carrying our own unchanged tail) — no clamping here, because clamping a mid-log position or match point would manufacture a bogus append or a phantom match; the leader's `onAcknowledgeEntries` feeds the follower's reported tail into `new TermIndex(...)` in **both** branches (the failure-branch rewind, the success-branch adoption), and a negative or missing tail threw before adoption, commit alignment or the catch-up send could run, now **refused as warn + ignore** (no reply exists on the ack path; connection book-keeping, log, and leadership are byte-for-byte untouched) | `stage 6(h)` |
| 6(i) | non-happy-path hardening (seventh pass): the last two hostile-wire-tail escapes of the 6(g)/6(h) family, now on the vote path — the follower's `onVoteRequest` now **refuses before any mutation** (the term-ladder conversion still runs first, then: warn + re-armed election timer + one explicit false `VoteResponse` carrying our own committed tail, mirroring the handler's own denial shape; `leaderId` never cleared, and a denial deliberately does **not** burn `votedForNodeId`, so a legitimate same-term candidate is still granted afterward) when the request's last-log term/index is a negative or missing wire field; the base-class `isUpToDate` — the single feed-in point all three pre-vote handlers share (follower/candidate/leader) — now refuses with a plain `false` (warn, no state change) on a hostile/missing tail, sealing all three roles with no handler-side change; `isHostileWireTermIndex` lifted to `RaftNodeBehavior` as a shared static (the follower's copy deleted; the three 6(h) append-path call sites resolve via the inherited static, unchanged) | `stage 6(i)` |
| 6(j) | non-happy-path hardening (eighth pass): the last hostile shape on the append surface, found by the post-6(i) audit sweep — a null slot **inside** the `AppendEntries.entries` list (the 6(h) pins covered a null term/index <i>inside</i> a well-formed entry; the wire ctor's element-blind `unmodifiableList` copy admits null slots, unlike `AnnounceClusterTopology`'s `List.copyOf`, which rejects such lists at construction) — `hasHostileWireTermIndex` now classifies a null slot as hostile, so the follower refuses it in the established shape (false ack carrying our unchanged tail; no leader installed, no append, no commit move, nothing applied) instead of NPE-ing on the very dereference the classification performs (the guard throwing where it exists to catch) — including the tail slot, which the accept path would read FIRST as the new tail, the post-mutation surface 6(h) closed on the other axes; the audit sweep clearing every remaining carrier (`AnnounceClusterTopology`, `VoteResponse`, `Introduction`, `StateResponse`) is recorded in §7 | `stage 6(j)` |
| 6(k) | non-happy-path hardening (ninth pass): the two base-class **bootstrap handlers** — `onIntroduction` / `onAnnounceClusterTopology`, the only surface the hostile-wire audit left, flagged in the §7 sweep as a *separate carrier family* (null id / null nodeAddress in the bootstrap carriers; `Introduction` and `messages.NodeIdentifier` are bare null-permitting, unlike the term/index family that the domain constructors reject) — both now **refuse before any mutation** (the no-reply refusal shape: warn, zero state change): an intro carrying a null id or null address is refused before `setRemoteNodeId`, before `register`, and before the announce reply (previously: a null-bound connection feeding the follower vote/leader-handoff paths, a null map key in the topology — a `HashMap` null key, which inflates `getMajorityCount()` and votes can never name the unnameable member, a quorum DoS — and the null-address entry propagating to peers inside the reply built from `getTopology()`); an announce with a null slot / null id / null address anywhere in its list is refused wholesale (the 6(i) whole-list convention) and an unbound sender must be resolvable against the held topology <i>or its announced list</i> (real nodes announce themselves, so legitimate bootstrap still resolves) — all checks now run **before** `register`, making the post-registration `locate` `orElseThrow` ISE an unreachable invariant tripwire that used to fire after the topology had moved (the throw-after-mutation surface, closed for the follower-append path in 6(j), now on the bootstrap surface); the "exhausted" claim of the 6(i)/6(j) closure is re-scoped to the term/index family (see §7) | `stage 6(k)` |
| 6(l) | non-happy-path hardening (tenth pass): the 6(k) identity family on the **vote surface as a whole** — the last audited vote surface the identity guard did not reach: all six handlers (follower/candidate/leader × `onVoteRequest`/`onVoteResponse`) now **refuse a null-sender before any term comparison or conversion** (one inline `sender.getRemoteNodeId() == null` check at the top of each, the 6(k) refusal shape: warn, zero state change, <i>no reply</i> — the vote surface has the reply machinery of the hostile-tail family available, but a grant-shaped reply would be strictly worse, so the family's no-reply form holds). Malformed-by-construction and unreachable on the live wire (`onIntroduction` is the only binder of `remoteNodeId`; `NodeConnectionOutboundImpl.receive` throws on an unbound non-bootstrap carrier) — defense-in-depth for the direct-handler surface (tests, middleware, future dispatch paths), where the handler-top check is the only line of defense. Three closed vectors: a follower grant recording `votedForNodeId = null`, which the same-term guard (`votedForNodeId != null`) <i>silently bypasses</i> — a second vote in the same term, Raft §5.2; a candidate tally via `votes.add(null)` (legal for a `HashSet`) — one phoney unattributed grant crossing `getMajorityCount()` (with the candidate's own vote already tallied, the null grant ALONE) into an illegitimate `convertToLeader`; and any-role step-downs handing free term recognition (a `convertToFollower(ForVote)` to an unattributable sender) before any guard hit | `stage 6(l)` |
| 6(m) | non-happy-path hardening (eleventh pass): the 6(k) identity family on the **append / pre-vote-response / install-snapshot surface** — the last audited handler surface the null-sender guard had not reached: the follower's `onAppendEntries` / `onPreVoteResponse` / `onInstallSnapshot` plus the candidate's and the leader's `onAppendEntries` now each **refuse a null sender before any term comparison, conversion, tally, grant, or install** (the 6(k) shape: warn, zero state change, no reply — a success-shaped ack would be strictly worse than silence). Malformed-by-construction and unreachable on the live wire (defense-in-depth for the direct-handler surface, as 6(l)); audit of the remaining surfaces (pre-vote request, candidate/leader pre-vote response, both ack paths, the leader's snapshot-response path) records why each needs no identity guard — see §7. Five closed vectors: a follower conversion with a high-term append under a null leader plus rollback of the uncommitted tail and a true `AcknowledgeEntries` to a connection nobody could name; a follower pre-vote tally where `preVoteGrants.add(null)` (legal for a `HashSet`) reaches the 3-node majority on ONE unattributed grant; a follower install-snapshot (payload adopt, log re-anchor, `SnapshotInstalled` publish, reply) for an unattributable leader; a candidate conversion to a null-leader follower before the re-dispatch even reaches the follower handler; and a live-leader step-down for an unauthenticated carrier | `stage 6(m)` |
| 6(n) | non-happy-path hardening (twelfth pass): the last open axis of the message-`field` null family the 6(k)–6(m) identity passes left — not the sender (6(k)–6(m)), not a hostile term/index (6(g)–6(i)), not a null <i>slot</i> (6(j)), but a null <i>payload</i> inside an otherwise well-formed append entry. The wire `AppendEntries.RaftLog` constructor null-guards `entry` (`requireNonNull`), which made the shape look unreachable — but Java deserialization reconstructs fields without calling constructors, so a hostile or corrupted stream carries the payload past the construction guard, and on the direct-handler surface that is the one vector that reaches the accept path with an entry the construction layer never accepted. Without the extension the refusal was never even reached: `RaftLogs.appendLogInternal` deletes conflicting entries FIRST (real log state torn out) and only then NPEs out of `CompletableRaftLog` construction — the throw-after-mutation family 6(e)/6(g)/6(h) closed on the other axes, here through a different field. `hasHostileWireTermIndex` now classifies `entry.getEntry() == null` in the same per-entry pass (alongside the 6(h) hostile term/index and the 6(j) null slot), so the follower's `onAppendEntries` refuses in the established 6(g)/6(h)/6(j) shape before ANY mutation — before `containsStartPoint`, before the leader install, before the append — one false ack answering in OUR term with OUR unchanged tail, zero state change (no leader, no append, no commit move, nothing applied). A null payload is structurally invalid exactly like a null slot or a missing term/index, so it is refused, not negotiated (no clamping); the full message-`field` null audit — the sealed matrix of every remaining carrier and why each needs no guard (consumer-inert reads, primitive-only fields, and the construction-guarded rest) — is recorded in §7 | `stage 6(n)` |
| 6(o) | non-happy-path hardening (thirteenth pass, audit sweep of the `SnapshotStore` file surface): the empty-<i>body</i> axis of the 6(d) family — a file whose NAME parses to a valid boundary but whose body is ZERO BYTES (`raft-snapshot-99-99.snap` in a shared directory, left by a crashed or foreign writer: both `save()`s reject an empty payload, so the shape cannot come from this store) — was being handed back as a `Stored` (the record null-checks the payload but not its length) and, named for a higher boundary than the real snapshot, MAX-BY-BOUNDARY masked the real one. `loadLatest` now warns and skips empty-payload files in the collect pass — parallel to the 6(d) unparseable-name skip (a hollow body is a no-snapshot, left in place, never allowed to blow up an otherwise clean load), and the `Stored` record completes the invariant symmetric with `save()` by rejecting an empty payload (defense-in-depth; the skip in `loadLatest` is the behavioral fix). Two pins: a hollow file named for a higher boundary is invisible to `loadLatest` (the real snapshot still wins, bytes intact, hollow file left in place) and a hollow-only directory loads as `Optional.empty()` instead of a `Stored` with an empty payload | `stage 6(o)` |
| 6(p) | non-happy-path hardening (fourteenth pass, the store `save()` argument surface on a 6(g)-family axis): the negative-<i>term</i> axis — the `TermIndex` constructor rejects BOTH negative axes (the 6(g) root mechanism), which made the axis look unreachable to the store guards, but a deserialized/construction-bypassed boundary reaches `save()` with one; both `save()`s guarded `boundary.getIndex() < 0` and never `boundary.getTerm() < 0`, so a `(-3,10)` boundary wrote `raft-snapshot--3-10.snap` — a name `parseBoundary` can never read back (the first dash is at position 0) — AFTER `deleteSuperseded` had torn out the valid snapshot: a "successful" save leaving the store EMPTY, data loss with no exception (worse than the 6(o) masking, where the real snapshot still existed); `InMemorySnapshotStore` stored the forged boundary and handed it back. Both `save()`s now refuse a negative term before any temp file, write, or delete (the shape of the sibling index guard), and the shared `Stored` record completes the axis (defense-in-depth, symmetrical with the 6(o) empty-payload completion) | `stage 6(p)` |

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
mechanism); and after stage 6(j): **354 tests green, 0 failures, 1 pre-existing
skip** (+2: two follower append-path refusal pins for the mechanism's last hostile
shape — a null <i>entry slot</i> in the entries list — closing it after an audit sweep
that cleared every remaining wire carrier on the term/index axes; see §7); and after
stage 6(k): **361 tests green, 0 failures, 1 pre-existing skip** (+7: five bootstrap
refusal pins and two legitimate-bootstrap acceptance pins on `onIntroduction` /
`onAnnounceClusterTopology`, the first test coverage of these handlers at all — a
separate carrier family, null id / null nodeAddress, from the term/index family the
audit closed; the "exhausted on the current surface" claim is re-scoped to that
family; see §7); and after stage 6(l): **368 tests green, 0 failures, 1 pre-existing
skip** (+7: the 6(k) identity family on the vote surface as a whole — null
`remoteNodeId` at the top of all six vote-surface handlers, refused before any term
comparison or conversion, no reply; the identity family now covers the bootstrap and
vote surfaces; see §7); and after stage 6(m): **373 tests green, 0 failures, 1
pre-existing skip** (+5: the 6(k) identity family on the append / pre-vote-response /
install-snapshot surface — null `remoteNodeId` refused at the top of the five remaining
audited handler sites (follower × append/pre-vote-response/install-snapshot, candidate
× append, leader × append), each pin paired with a bound-sender discriminator proving
the legitimate path survives; the identity family now covers every audited handler
surface; see §7); and after stage 6(n): **374 tests green, 0 failures, 1 pre-existing
skip** (+1: the message-`field` null family closes its last axis — a null <i>payload</i>
inside an otherwise well-formed append entry, construction-guarded and thus apparently
unreachable, reached only via a deserialized/corrupted stream or the direct-handler
surface, refused before the delete-then-NPE throw-after-mutation the accept path would
have run; the full sealed matrix — sender, term/index, slot, and the ruled-out
carriers — is recorded in §7; see §7). After the 6-stage series closed: the
6(d) read-only test's non-POSIX (Windows) portability gate — a clean JUnit skip
instead of a hard `UnsupportedOperationException` error, test-only, no production
code, tally **unchanged on Linux** (the test still runs and is green there) — was
recorded as its own change, `Windows portability`; see §7; and after stage 6(o): **376 tests
green, 0 failures, 1 pre-existing skip** (+2: the empty-<i>body</i> axis of the
6(d) family — a valid-named zero-byte snapshot file in a shared directory is now
a no-snapshot that `loadLatest` warns and skips instead of a `Stored` that
max-by-boundary could use to MASK the real one; the `Stored` record also completes
the invariant symmetric with `save()`; see §7); and after
stage 6(p): **379 tests green, 0 failures, 1 pre-existing
skip** (+3: the negative-<i>term</i> axis of the 6(g) root mechanism
reaching the store `save()` argument surface — while the `TermIndex`
constructor rejects both negative axes (which is exactly what made this
axis look unreachable), both `save()`s guarded only the index, so a
deserialized/construction-bypassed `(-3,10)` boundary would write
`raft-snapshot--3-10.snap` — a name `parseBoundary` can never read back —
in a `FileSnapshotStore` that has already deleted the valid snapshot
(data loss with no exception), or been handed back by
`InMemorySnapshotStore.loadLatest`; both stores now refuse it before any
write or delete, and the `Stored` record completes the axis as
defense-in-depth; see §7).
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

**Non-happy-path hardening pass, eighth round (2026-11-23, stage 6(j))** — after 6(i)
sealed the `TermIndex` feed-in family, the audit continued over the rest of the wire
surface: every list-shaped field and every remaining data-only carrier the term/index
guards never touched. It cleared four; one last structurally hostile shape survived.

**The audit sweep (cleared carriers).** `AnnounceClusterTopology.nodeIdentifiers` —
rejected at the boundary: the ctor's `List.copyOf` NPEs on a null slot, so a hostile
list never reaches the handler (unlike `AppendEntries`, whose `unmodifiableList` copy
is element-blind). `VoteResponse.committedTermIndex` on the collector side — all three
consumer handlers (follower/candidate/leader `onVoteResponse`) read only `getTerm()`
and the grant flag; the reported tail is never dereferenced. `Introduction.lastReceivedTermIndex` —
a write-only field: `getLastReceivedTermIndex` has zero caller sites outside its own
class (grep-verified), so nothing feeds it into the domain `TermIndex` ctor.
`StateResponse`'s term/index fields — constructed from our **own** non-null log values,
and the one consumer (`RaftNodeImpl.getLeader`) reads only the primitive `int term`.
None of these can escape as a hostile value.

**The last shape — follower `onAppendEntries` (production change, one line).** A null
*slot inside* the entries list. The 6(h) pins cover a null term/index *inside a
well-formed entry*; a null slot is a different shape the wire admits:
`AppendEntries` copies its list element-blind (`Collections.unmodifiableList`), so
`Arrays.asList(x, null)` is a constructible, wireable message — and the accept path
dereferences **every** slot (each entry's term/index at append, the last entry's
term/index as the new log tail, every entry again in the append loop). Without the
slot check there is no clean refusal to speak of: a slot mid-list makes the guard
itself NPE **while classifying** (`entry.getTermIndex()` on null) — the guard throwing
where it exists to catch — and a slot at the tail is worse, the one the accept path
reads **first** as its new tail, i.e. the post-mutation surface 6(h) closed on the
other axes. The fix is one line: `entry == null` is classified hostile in
`hasHostileWireTermIndex`. The refusal shape is unchanged (the 6(h) convention): one
false `AcknowledgeEntries` carrying our own unchanged tail, zero state change — and the
guard's warn now says "invalid term, index or entry slot". The leader's ack path
needed no change: an acked tail is a *single* wire `TermIndex`, already 6(h)-sealed,
and there is no list-typed ack field.

**Pins, two total**, in `RaftNodeBehaviorFollowerInstallSnapshotTest` (the 6(h) host),
immediately after the 6(h) append pins. (1) *null slot first*, then a well-formed
entry: the classification pass NPEs on the first slot without the check — this pin
asserts the refusal is answered, false, carrying our unchanged epoch tail (0), with no
leader installed and the log tail / commit point / applied index all still 0. (2)
*null slot at the tail*: this is the slot the accept path reads first as its new tail
(`entries.get(size−1)`) — the post-mutation-order surface — and the same zero-state
contract is asserted.

Isolated runs, both 6(h) host classes: follower **25 green** (23 → 25), leader
**24 green** unchanged — the inherited static resolves from the follower's append-path
guard, regression-verified as in 6(i). Final suite: **354 tests green, 0 failures, 1
pre-existing skip** (+2 over the 6(i) baseline).

**Closure.** Every wire carrier is now accounted for and the hostile-wire family —
structurally invalid term/index/entry values that would throw or corrupt mid-handler —
is exhausted on the current surface <i>for the term/index family</i>, including the one
shape that was neither guardable by clamping (nothing to clamp to) nor caught by the
term/index guards (it is the *absence* of an object). The re-scoping is made explicit
in the 6(k) round below: the bootstrap identity carriers closed there are a different
mechanism. The handoff stands as recorded in 6(i): the next items on the plan are the
structural backlog — (A) spill-to-disk durable store, (B) partition ownership,
(C) fixed-cluster topology, (D) infra debt.

**Non-happy-path hardening pass, ninth round (2026-11-23, stage 6(k))** — the
term/index family closed in 6(g)–6(j) was one carrier family, not the whole hostile
surface. The post-6(j) audit flagged the base-class **bootstrap handlers** —
`onIntroduction` and `onAnnounceClusterTopology`, the two handlers through which
every legitimate cluster is formed — as accepting a *separate* family: bare
null-permitting identity fields. The 8th-round sweep's "carriers cleared" was true of
the term/index <i>axes</i> of those carriers (`Introduction`'s log tail there is
write-only), but neither `Introduction` nor `messages.NodeIdentifier` null-guards its
id or its address object — `topology.NodeAddress` null-guards the address *string* in
its ctor, which is not the hostile shape; a null `NodeAddress` <i>inside the
identifier</i> passes through untouched and reaches the handler. And unlike the
term/index carriers, these shapes do not IAE out of a domain constructor, because they
never feed one in: they poison the **map** (null keys) and the **stream filter**
(null dereferences out of `locate`).

**The two escape mechanisms (both closed now, both pre-mutation).** (1) *null id →
quorum DoS.* `ClusterTopology.register` puts the entry into a `HashMap` keyed by id:
a null key is *legal*, `getMajorityCount()` is `(size/2)+1`, so a single null entry
on a 1-node cluster flips the majority 1→2 — and no vote, pre-vote grant, or commit
ack can ever name the unnameable member (`RaftNodeBehaviorFollower`'s
`votedForNodeId = sender.getRemoteNodeId()`, `preVoteGrants` majority math,
`RaftNodeBehaviorCandidate`'s `votes >= getMajorityCount()` all consume it). The
binding side is equally broken: `sender.setRemoteNodeId(null)` hands a null-id
connection to the follower's leader-handoff paths (`convertToFollowerForNewLeader`).
(2) *null address → NPE out of `locate`.* `onAnnounceClusterTopology` registered the
whole announced list **before** its first `locate()`, which resolves an unbound
sender: the stream filter dereferences every entry's address, so one null-address
entry NPEd the handler **after the topology had already moved** — the
throw-after-mutation shape 6(j) closed on the follower-append surface, on the
bootstrap surface. The tail `locate(...).orElseThrow(ISE)` was the same family: an
unbound sender whose address appears neither in the topology nor in the announced
list threw **after** registration.

**The fix (two handlers, two helpers).** Neither handler has a reply to carry a
refusal, so both take the no-reply shape (the 6(h) leader-ack precedent): warn, zero
state change, no exceptions. `onIntroduction` now refuses a null id or null address
**before** `setRemoteNodeId`, **before** `register`, and **before** the announce
reply. `onAnnounceClusterTopology` now refuses the entire announce if any slot is a
null entry/id/address (the 6(i) whole-list convention — refuse the list, not
"register the valid entries and throw on the bad one"), and additionally requires an
unbound sender to be resolvable against the held topology **or the announced
list** — a real node announces itself, so the legitimate bootstrap (the sender's
address present inside its own list) still resolves — and all of these checks run
**before** `register`, making the post-registration `locate` a guaranteed hit: the
`orElseThrow(ISE)` survives as an unreachable invariant tripwire rather than as a
reachable refusal path. Helpers sit next to `isHostileWireTermIndex` as pattern
siblings: `isInvalidNodeIdentifier(id, address)` and `canResolveAddress(address,
announced)` (null-safe; checks the held topology, then the announced list).

**Pins, seven total**, in `RaftNodeBehaviorFollowerInstallSnapshotTest` — the first
test coverage of these two handlers (grep-verified zero before this stage; the only
end-to-end exercise was the full-cluster `RaftNodeTest` bootstrap, which stays green
at 155). Refusals: (i) intro with null id — no `setRemoteNodeId` binding, no
registration (size 1→1, majority 1→1), no reply; (ii) intro with null address — same
shape, and no null-address entry leaks into the topology to ride out in the reply to
peers; (iii) announce with a well-formed peer entry <i>plus</i> a null-id entry — the
whole announce is refused, even the valid entry is not registered, quorum untouched,
sender untouched; (iv) announce with a null-address entry from an unbound sender —
no NPE out of `locate`, nothing registered, sender unbound (previously: registered,
*then* NPE); (v) well-formed announce from an unbound sender whose address matches
neither the topology nor its own list — no ISE, nothing registered (previously:
registered, *then* ISE). Acceptance regressions: (vi) a valid intro binds the
connection to the introducer, registers it, and replies with exactly one announce
carrying both the self entry and the introducer's; (vii) an unbound sender that
announces itself resolves, binds, and registers exactly once. The pin host's
`RecordingSender` had to change two stubs from throw/no-op to <i>record</i> (the old
`setRemoteNodeId` threw UOE; `sendAnnounceClusterTopology` was a silent no-op) —
safe because no test had ever reached either (the handlers were uncovered), and the
refusal pins now assert call/no-call rather than "did it throw".

Isolated: the host **32 green** (25 → 32); neighbors `RaftNodeBehaviorFollowerVoteTest`
**3 green** and the full-cluster `RaftNodeTest` **155 green** — legitimate bootstrap
through the real `sendIntroduction` path unchanged. Final suite: **361 tests green,
0 failures, 1 pre-existing skip** (+7 over the 6(j) baseline).

**Reconciliation of the "exhausted" claim.** The 6(i)/6(j) closure was scoped to the
**term/index family** — wire values that feed a domain `TermIndex` comparison or an
`entries` slot whose absence is the hostility. The bootstrap identity family (null
id / null address) is a different mechanism, closed by this stage; the wire surface
is now exhausted *per family*, and both families are accounted for on the current
surface. The structural-backlog handoff stands as recorded in the 8th-round closure.

**Non-happy-path hardening pass, tenth round (2026-11-23, stage 6(l))** — 6(k) closed
the identity family on the **bootstrap surface**; the post-6(k) sweep found the same
carrier — a bare `Sender` whose `getRemoteNodeId()` is null — still unguarded on the
**vote surface as a whole**: all six handlers (follower/candidate/leader ×
`onVoteRequest`/`onVoteResponse`) read the sender's identity on their mutation paths
with no top-of-handler check. On the direct-handler surface (tests, middleware, future
dispatch paths) that identity is the only attribution, and three vectors were live:
(1) *follower second-vote* — a grant records `votedForNodeId = sender.getRemoteNodeId()`
= null, and the same-term one-vote-per-term guard (`votedForNodeId != null`)
<i>silently bypasses null</i>, so the follower hands out a second vote in the same term
(Raft §5.2 violation, and the grant's `voted` flag is a positive — a real node can
never get the refusal back); (2) *candidate illegitimate leadership* —
`votes.add(sender.getRemoteNodeId())` = `votes.add(null)`, legal for the `HashSet`;
with the candidate's own seeded vote already tallied, one phoney null-sender grant
<i>alone</i> reaches `getMajorityCount()` (2 of 2 on the test cluster) and
`convertToLeader` fires with a majority no real peer named; (3) *free step-down
anywhere* — every `> term` branch runs `convertToFollower[ForVote]` on the unbound
sender before any other guard hit: a null-sender carrier hands free term recognition
to a voter/connection nobody could name, and on the request path the follower grant
to that carrier compounds (1). Unreachable on the live wire — `onIntroduction` is the
only binder of `remoteNodeId` (6(k)) and `NodeConnectionOutboundImpl.receive` throws
on an unbound non-bootstrap carrier — so this is defense-in-depth for the
direct-handler surface, in the identical shape 6(k) set for the identity family:
warn, **zero state change**, **no reply** (the vote surface has reply machinery — the
6(i) hostile-tail family uses it — but a *grant-shaped* reply would be strictly
worse than silence: a null grant with a false `voted` flag is still a message that
names no one and a positive one is a lie, so the family's no-reply form holds).

**The fix (six handlers, one inline check each).** Each of the six handlers now
starts with `sender.getRemoteNodeId() == null` → warn (naming the handler, the
sender's `getRemoteNodeAddress()` — "stub-null" for a stub) + return, **before any
term comparison, vote-record write, tally write, or conversion**. No new helper:
the check is one line, the six sites are semantically distinct enough that a shared
predicate would hide the per-handler message, and the family's convention (6(k):
inline, message-rich) holds. Inline, not in the base class, because the six handlers
are the only sites and the base already hosts the bootstrap handlers' two
6(k) checks — a third helper family in the base would obscure the per-handler
attribution each message carries.

**Pins, seven total**, one per guarded handler site, across the three role hosts
(the pin set follows the role-naming convention of `RaftNodeBehaviorFollowerVoteTest`
and its siblings; the candidate and leader hosts are <i>new files</i>, mirroring the
role naming, since no existing candidate- or leader-role vote host existed — the
pre-vote hosts are a different mechanism and were not the right place). Follower, in
`RaftNodeBehaviorFollowerVoteTest` (+2): (i) unbound request at the same term —
refused, no `votedForNodeId` write, no reply; the discriminator follows: a bound
same-term candidate is still granted (the refusal did not burn the vote and did not
kill the handler), and the same-term <i>second</i> bound candidate gets the
vote-burn refusal — the one-vote-per-term machinery is intact <i>and</i> now immune
to the null bypass; (ii) unbound response at a higher term — refused, no term
advance (an unattributable carrier must not be granted free term recognition); the
following bound higher-term response still converts the follower. Candidate, in the
new `RaftNodeBehaviorCandidateVoteTest` (+3): (iii) unbound grant — the sharpest
discriminator in the set: without the guard, own-seeded-vote + null = 2 of 2 and the
candidate would <i>be leader</i> after this single dispatch, so the
`CANDIDATE`-after-dispatch assertion is the guard itself; then ONE real grant (own +
peerA = 2 of 2) steps it up legitimately; (iv) unbound higher-term response — no
step-down, term unchanged; a bound higher-term response still converts to follower;
(v) unbound higher-term request — no conversion to follower (the `> term` branch
refuses before the conversion), term unchanged; a bound higher-term request still
converts and grants. Leader, in the new `RaftNodeBehaviorLeaderVoteTest` (+2):
(vi) unbound higher-term request — no step-down; a bound higher-term request still
converts (the legitimate handoff is alive); (vii) unbound higher-term response — no
step-down; a bound higher-term response still converts. Each pin's
`assertTrue(sender.voteRequests().isEmpty())` / `voteResponses().isEmpty()` is the
no-reply assertion (identity-family shape: nothing goes back to the unattributable
sender).

Isolated: `RaftNodeBehaviorFollowerVoteTest` **5 green** (3 → 5), the new
`RaftNodeBehaviorCandidateVoteTest` **3 green**, the new
`RaftNodeBehaviorLeaderVoteTest` **2 green**,
`RaftNodeBehaviorPreVoteTest` **13 green** (unchanged — the pre-vote surface is a
different mechanism and was out of scope), all with the guards firing (the warn
lines — "is unbound (null remoteNodeId) - refusing" — visible once per driven
dispatch in the surefire output). Full-cluster `RaftNodeTest` **155 green** —
legitimate wire traffic always binds through `onIntroduction` before any vote
carrier, e2e untouched. Final suite: **368 tests green, 0 failures, 1 pre-existing
skip** (+7 over the 6(k) baseline of 361).

**Non-happy-path hardening pass, eleventh round (2026-11-23, stage 6(m))** — 6(k)
closed the identity family on the **bootstrap surface**; 6(l) carried it to the
**vote surface**; the post-6(l) sweep enumerates every remaining audited handler
surface and finds the same carrier — a bare `Sender` whose `getRemoteNodeId()` is
null — still unguarded on the **append / pre-vote-response / install-snapshot
surface**, at exactly the five sites whose mutation paths read the sender's identity
with no top-of-handler check: the follower's `onAppendEntries`, `onPreVoteResponse`,
and `onInstallSnapshot`, plus the candidate's and the leader's `onAppendEntries`. On
the direct-handler surface (tests, middleware, future dispatch paths) those five
mutations are unattributable in the same shape 6(l) closed: (1) *follower
conversion plus true ack* — a higher-term append converted the node (term advance,
rollback of the uncommitted tail) with `leaderId` still null, then the accept path
appended and delivered a true `AcknowledgeEntries` to a connection nobody could name;
(2) *follower pre-vote tally* — `preVoteGrants.add(null)` is legal for the
`HashSet`, and in a 3-node cluster 1 (self) + \{null\} = 2 = `getMajorityCount()`, so
ONE unattributed grant alone crossed it into `convertToCandidate` (set semantics cap
the inflation at one — but one is exactly majority-reachable in three nodes); (3)
*follower install-snapshot* — payload adoption, log re-anchor to the snapshot
boundary, the `SnapshotInstalled` publish to the state machine, and the
`InstallSnapshotResponse` reply, all on behalf of a leader nobody could name (the
wire message always self-claims one — `InstallSnapshot`'s constructor rejects a null
`leaderId` — so the guarded axis is the sender's identity, precisely, not the wire's);
(4) *candidate conversion* — `convertToFollowerForNewLeader` (term advance, rollback,
behavior swap) ran before the re-dispatch even reached the follower handler, then the
ack went out to the unattributable carrier; (5) *leader step-down* — the live leader
demoted, term advance plus rollback, for an unauthenticated message. Same wire
invariant as 6(k)/6(l): `onIntroduction` is the sole binder of `remoteNodeId` and
`NodeConnectionOutboundImpl.receive` throws on an unbound non-bootstrap carrier, so
the five handler-top checks (before any term comparison, conversion, tally, grant,
or install) are defense-in-depth for the direct-handler surface, in the shape the
family established: warn, zero state change, no reply (refusing with a
success-shaped ack would be strictly worse than silence).

**Surfaces audited and ruled out** (the sweep recorded each with the reason it needs
no identity guard): `onPreVoteRequest` on all three roles — grant-only, the only
mutation is the `PreVoteResponse` reply computed from the *request's* term and
`isUpToDate` (6(i)-closed axes); identity is log-only, and its grant pins already
live in `RaftNodeBehaviorPreVoteTest`; candidate and leader `onPreVoteResponse` —
pure warn + ignore (pre-vote responses never change state by semantics), identity
log-only; `onAcknowledgeEntries` (leader and follower) — the mutation axes are the
term (the step-down is `convertToFollower(term, …)`, which names *no leader*, so the
null-leader vector of this pass does not exist on this surface) and the reported
tail (6(h)-closed), with the remaining identity usage being per-connection
`setTermIndex` book-keeping on the carrier and log lines — no cluster-state axis
reads the sender's identity (the commit recompute runs over bound connections);
the leader's `onInstallSnapshotResponse` — its step-down axis is likewise
`convertToFollower(term)` with no leader naming (contrast the guarded
`convertToFollowerForNewLeader`), its cursor/term/index axes are 6(d)/6(e)/6(g)-
closed, and its residual identity usage is log lines, per-connection book-keeping,
and `snapshotsInFlight.get(null)` → null → the clean "no transfer in flight"
completion (bound-connection-driven continuation); and the follower accept path's own
`leaderId != null && !leaderId.equals(sender…)` rejection is intrinsically null-safe
on the identity axis — its *other* branch, the `leaderId == null` install, is
precisely what the new handler-top check now prevents for an unbound sender.
Placement note on the follower's pre-vote response: the guard sits ahead of the
stale-round check, while the tally is only reachable in an active round — rounds are
opened solely by the follower's election-timer path (`onElectionTimeout` arming the
round and dispatching the term+1 hypothetical votes), so a late unbound grant for a
concluded round is refused by the identity guard, not the stale-round branch, and a
bound grant is only ever counted inside an active round — the pin drives both sides.

**Pins, five total**, one per guarded site, across the four stage hosts (every
conversion vector carries a bound-sender discriminator — the SAME message from a
BOUND sender still converts / steps down, so the legitimate handoff is proven alive;
the candidate and leader discriminators accept at the fresh-epoch prev/commit — the
conversion's rollback of the uncommitted tail is exactly what a legitimate
re-syncing leader presents): `RaftNodeBehaviorFollowerInstallSnapshotTest` +2 —
`unboundSenderAppendEntries_higherTerm_doesNotConvert` (stays FOLLOWER/term 0, no
ack; the bound-sender discriminator converts to term 7 under the recorded leader and
is acknowledged) and `unboundSenderInstallSnapshot_higherTerm_neitherConvertsNorInstalls`
(no reply, no snapshot boundary, no state-machine adoption; the bound sender then
installs to (9,20) and adopts the payload); `RaftNodeBehaviorPreVoteTest` +1 —
`follower_collector_unboundSenderGrant_neverCounts` (the unbound in-round grant is
refused with no reply, the node stays FOLLOWER/term 0; a bound grant flips it to
CANDIDATE/term 1 lawfully, a second bound grant is counted and inert);
`RaftNodeBehaviorCandidateVoteTest` +1 — `unboundSender_appendEntries_higherTerm_doesNotConvert`
(stays CANDIDATE/term 1, no ack; the bound-sender discriminator steps it down to
FOLLOWER/term 2 under the recorded leader and is acknowledged);
`RaftNodeBehaviorLeaderVoteTest` +1 — `unboundSender_appendEntries_higherTerm_doesNotStepDown`
(the live leader keeps its state and does not acknowledge; the bound-sender
discriminator steps it down to FOLLOWER/term 2 with a true success ack).

Isolated: the four stage 6(m) host classes **55 green** (PreVote 14,
FollowerInstallSnapshot 34, CandidateVote 4, LeaderVote 3) with the guards firing on
every driven dispatch (the warn lines — "is unbound (null remoteNodeId) - refusing"
— visible once per dispatch in the surefire output). Final suite: **373 tests
green, 0 failures, 1 pre-existing skip** (+5 over the 6(l) baseline of 368).
The structural-backlog handoff stands as recorded in the 8th-round closure.

**Non-happy-path hardening pass, twelfth round (2026-11-23, stage 6(n))** — the
message-`field` null family: 6(g)–6(i) closed the hostile term/index axes, 6(j) the null
<i>slot</i> axis, and 6(k)–6(m) the sender-identity family. The post-6(m) field audit
walks every `AppendEntries` carrier field the follower's `onAppendEntries` accepts and
rules each axis out on its own terms — `previousLogTermIndex` /
`leaderCommitTermIndex` / each entry's `termIndex` are already classified by
`hasHostileWireTermIndex` (6(g)/6(h)/6(i)) and refused at handler entry BEFORE any
mutation; a null <i>slot</i> is already hostile (6(j)); and the one remaining
unclassified shape — a well-formed entry (clean term/index, present slot) whose
<i>payload</i> is null — is the one the wire constructor refuses to build
(`AppendEntries.RaftLog`'s `requireNonNull(entry)`), which looked like a non-vector.

It is not. The construction guard is a property of construction: Java deserialization
reconstructs fields straight off the stream WITHOUT calling the constructor (the exact
bypass this codebase's own `WireSerializationTest` relies on to round-trip its
carriers), and the direct-handler surface (tests, middleware, a future
deserialize-and-dispatch path) can hand a handler an entry the constructor never
saw. That is the one vector that reaches the accept path carrying an entry the
construction layer never accepted — and it is the worst kind this project has already
closed, because the failure is not in the guard, it is in the order of the accept
path itself: `RaftLogs.appendLogInternal` deletes conflicting entries FIRST (real log
state torn out — the throw-after-mutation surface 6(e)/6(g)/6(h) closed on the
term/index axes) and only then hands the entry to `CompletableRaftLog` construction,
whose `super` call trips `RaftLog`'s `requireNonNull(entry)` and throws. The deletion
is committed before the NPE fires; the node is left with its conflicting entries gone
and an uncaught throw where a refusal should have been.

So the guard moves ahead of the accept path, exactly as the family established:
`hasHostileWireTermIndex` classifies a null payload in the same per-entry pass as
the null slot and the hostile term/index —

    for (AppendEntries.RaftLog entry : appendEntries.getEntries()) {
        if (entry == null || isHostileWireTermIndex(entry.getTermIndex()) || entry.getEntry() == null) {
            return true;
        }
    }

— and `onAppendEntries` refuses it in the established 6(g)/6(h)/6(j) shape at the
classification call, BEFORE `containsStartPoint`, BEFORE the leader install, BEFORE
the append. One false ack answering in OUR term with OUR unchanged tail, zero state
change (no leader, no append, no commit move, nothing applied). A null payload is
structurally invalid exactly like a null slot or a missing term/index, so it is
refused, not negotiated — no clamping, because there is no legal value to clamp to.
The method javadoc and the inline comment both carry the 6(n) axis and its
destructive-order rationale so the next auditor sees why the check must stay in the
classification pass, not the accept branch.

**Pin, one total:** `RaftNodeBehaviorFollowerInstallSnapshotTest`
`appendCarryingANullEntryPayload_isRefusedWithoutThrowing` — a well-formed `(0,0)`
entry whose payload is forged null via reflection on `AppendEntries.RaftLog.entry`
(the exact shape a deserialized hostile stream would deliver, and the one shape the
constructor forbids — the pin must bypass construction, so the guard is proven on
this vector, not on a construction-time NPE). Asserts the family contract: one ack,
`isSuccess == false`, tail at our epoch (term 0), no leader installed,
`logs.getCurrentTermIndex().getIndex() == 0`,
`logs.getCommittedTermIndex().getIndex() == 0`,
`bridge.getLastAppliedIndex() == 0` — the 6(h)/6(j) null-index and null-slot
siblings stay green in the same class, so the null-payload axis is isolated, not
conflated with its predecessors.

Isolated: `RaftNodeBehaviorFollowerInstallSnapshotTest` **35 green** (34 baseline +
1 pin), the guard firing on the driven dispatch in the surefire output. Final suite:
**374 tests green, 0 failures, 1 pre-existing skip** (+1 over the 6(m) baseline of
373). The message-`field` null family is now closed across all carriers and all
axes: the sealed matrix (term/index 6(g)–6(i), slot 6(j), payload 6(n), sender
6(k)–6(m)) is recorded in the stage table and above.

**Test portability fix, recorded after the 6(n) closure (2026-11-23 — separately
tracked, test-only, `Windows portability`)** — the 6(d) read-only-directory test
(`FileSnapshotStoreTest#saveIntoAReadOnlyDirectory_failsLoudlyAndLeavesNoTempFile`)
builds its failure premise with `Files.setPosixFilePermissions`, which the JDK
documents as throwing `UnsupportedOperationException` on filesystem providers without
POSIX attributes (Windows: `setPosixFilePermissions` line 2166). That is a hard
ERROR on every non-POSIX run, not a skip — and skipping is the only correct answer,
because the precondition itself (a directory this user genuinely cannot write) is not
constructible on those platforms: there is no NIO permission bit to clear. The fix
probes FIRST — `Files.getFileAttributeView(dir, PosixFileAttributeView.class)`
returns the view on POSIX (Linux/Mac) providers and null (the documented contract for
an unsupported view; never an exception) elsewhere — and a single
`Assumptions.assumeTrue(view != null)` turns the non-POSIX case into a clean JUnit
skip before any permission mutation. The probe is therefore platform-name-free (it
asks the filesystem, not `os.name`) and runs ahead of both the `setPermissions`
helper calls (which now take the view — `attributeView.setPermissions(...)`) and the
existing ROOT gate (a user who can still write a read-only directory — the test's
premise does not hold — still a clean skip; unchanged). The test body, the save/
no-temp-file contract, and the restore in `finally` are otherwise untouched, and no
production code changed. Verification: isolated `FileSnapshotStoreTest` **11 green /
0 fail / 0 skip** (both gates pass on this Linux / uid-1000 box, the test still runs
fully), full suite **374 tests green, 0 failures, 1 pre-existing skip** — the tally
is unchanged on Linux; the gate only changes the outcome ON NON-POSIX platforms,
where the test now SKIPs by design instead of ERRORing on every run. The "1
pre-existing skip" of the 6(k)–6(n) series is a different test (the read-only premise
holds on this Linux box, so it runs and is green).

**Non-happy-path hardening pass, thirteenth round (2026-11-24, stage 6(o))** — the
empty-<i>body</i> axis of the `SnapshotStore` file surface. With the message-`field`
null family sealed, the audit turned to the store-level surface 6(c)/6(d) had
established — `FileSnapshotStore.loadLatest` — and found the one residual hole in its
"looks like a snapshot, is not one" contract: a file whose NAME parses to a valid
boundary but whose BODY is EMPTY. Both `save()`s reject `payload.length == 0` (an
empty payload is a caller error, so the shape cannot come from this store), which is
exactly why this is the 6(d) threat model — a crashed writer or a foreign tool in a
shared directory — one step along its axis. The old behavior was the 6(c)/6(d) failure
re-run on the body instead of the name: `parseBoundary` (the 6(d) guard) passed,
`readAllBytes` returned `new byte[0]` (non-null), `new Stored(...)` passed (the record
null-checked the payload, not its length), and `loadLatest` happily returned a hollow
`Stored` — a state a `save()` can never produce. The masking pin shows the sharp form:
the hollow file NAMES a higher boundary than the real snapshot, so the max-by-boundary
selection (the 6(d) invariant) picked the EMPTY one and the real one was masked — a
silent wrong-selection, not a throw, which is precisely why it evaded the earlier
passes (nothing blew up; the wrong file simply won).

The fix stays in the same place the family established — classify at collection, never
mid-accept:
- `loadLatest`'s collect pass now probes a `hasPayload` (a `Files.size > 0`, not a
  read — cheap, and deliberately codec-free: collection does not decode, the consumer
  already degrades on undecodable bytes) AFTER the 6(d) name check, and a zero-byte
  file is WARNED and SKIPPED, left on disk (load is read-only, the 6(d) contract), so
  "latest among VALID snapshots" still holds. A file that has vanished or cannot be
  stat'ed is treated as hollow (skip) — never allowed to masquerade as a valid one.
- The `Stored` record in `SnapshotStore.java` now ALSO rejects `payload.length == 0`,
  symmetric with `save()`'s null-and-empty rejection. That is defense-in-depth one
  layer up, not the behavioral fix (the `loadLatest` skip is the behavior); it closes
  the door the record left open (null checked, length unchecked) so a future
  implementation that constructs a `Stored` by hand cannot reintroduce the hollow
  shape.

**Pins, two total:** `FileSnapshotStoreTest#aHollowSnapshot_namedForAHigherBoundary_isInvisibleToLoadLatest`
— a real snapshot at `(5,10)` plus a zero-byte `raft-snapshot-99-99.snap`; asserts the
boundary is still `(5,10)`, the payload bytes intact, and the hollow file LEFT IN
PLACE (read-only). And `FileSnapshotStoreTest#aDirectoryWhoseOnlySnapshotIsHollow_loadsAsEmpty`
— a directory whose only file is a zero-byte snapshot must load as `Optional.empty()`
(the fresh-store contract), not a hollow `Stored` and not an exception.

Isolated `FileSnapshotStoreTest`: **13 green / 0 fail / 0 skip** (11 baseline + 2
pins; the read-only test still runs green on this Linux box). Full suite at this
point: **376 tests green, 0 failures, 1 pre-existing skip** (+2 over the 6(n)
baseline of 374). The file-surface family is now closed across all three axes
6(c)/6(d)/6(o) map it — a name that does not parse (6(d)), a body that is empty
(6(o)), and the max-by-boundary selection invariant both rely on — with `save()`,
`loadLatest`, and the `Stored` record agreeing on what a legitimate `Stored` is.

**Non-happy-path hardening pass, fourteenth round (2026-11-24, stage 6(p))** — the
negative-<i>term</i> axis of the 6(g) root mechanism on the store `save()`/`Stored`
surface. 6(g) established the root mechanism (`TermIndex`'s constructor throws IAE on
a negative term or index, and a consumer feeding an unguarded wire value into it
breaks mid-mutation) and 6(g)–6(i) sealed every message-handler feed-in the sweep
could find. The 6(o) sweep then ruled the `save()` input surface sealed: `save` valid­ates
boundary (null, negative index) and payload (null, empty) BEFORE any temp file, any
write, any delete — and `TermIndex`'s constructor rejecting BOTH negative axes was taken
as the reason no guard for the term axis was needed. That reasoning holds only for
constructor-built boundaries: the 6(n) pass established that deserialization (the same
mechanism the 6(n) pin forges) reconstructs fields WITHOUT calling constructors, so a
construction-bypassed `TermIndex` can carry a negative `term` past the very guard that
made the axis look sealed. The store guards checked `boundary.getIndex() < 0` and
NEVER `boundary.getTerm() < 0`; the shared `Stored` record had the same one-sided
check. This is the 6(n) shape (a guard that looks like a non-seal, reachable only via
the construction-bypass path) applied to the 6(g) axis — and like 6(g) the consequence
is silent data loss, not merely a wrong value:

- `FileSnapshotStore.save` would write the boundary `(-3,10)` as
  `raft-snapshot--3-10.snap`. `parseBoundary` splits on the FIRST dash and finds it at
  position 0, so `Integer.parseInt("")` throws before the boundary is even compared to
  a valid one — a name that no reader, ever, can recover (the 6(d) skip would WARN on
  it, but that is diagnosis, not recovery). And because `deleteSuperseded` runs BEFORE
  the write (the retain-latest contract, stage 6b), the previously valid snapshot is
  torn out before the unreadable write would become discoverable. The "successful"
  `save` left an EMPTY store and a directory holding a file no one can load: a forged
  boundary deleting real state with no exception — strictly worse than the 6(o)
  masking, where at least the real snapshot still existed, merely not selected.
- `InMemorySnapshotStore.save` accepted the forged boundary into the single slot and
  `loadLatest` handed it back — a `Stored` naming a boundary no state machine can have
  (terms are non-negative; the epoch is `(0,0)`). The record's 6(o) completion closed
  the payload-length door one step before; the term door was the one left open.

The fix is the 6(o) layered shape on the new axis — behavioral guard at the entry,
record completion as defense-in-depth:

- Both `save()`s (`FileSnapshotStore`, `InMemorySnapshotStore`) now refuse
  `boundary.getTerm() < 0` with IAE BEFORE any temp file, any directory listing, any
  delete, any slot write — in the shape of the sibling index guard already present, so
  the guard set no longer depends on `TermIndex`'s constructor being the only
  structural check the store can rely on (it was not, on the 6(n) axis).
- The shared `SnapshotStore.Stored` record (null boundary, null payload, empty payload
  after 6(o), negative index) now also rejects a negative term. The record is the
  store-INDEPENDENT line of defense: a future `SnapshotStore` implementation whose
  `save` forgets the axis still cannot hand the shape back through a record.

Ordering is the load-bearing detail and it shaped the pin: the refusal must land at
the guard (step 1 of `save`), not the delete step. The pre-fix data-loss path was
validate → delete superseded → write, failing at step 3 on an input that was invalid
at step 1. The 6(o) pins asserted the hollow file survives; this pin asserts the
OPPOSITE survivor — the pre-existing VALID snapshot must still load — because the
pre-fix path deletes it. A "leaves no file" assertion would have passed against the
pre-fix delete-then-write order; asserting the valid snapshot's survival is what
distinguishes a refusal from the data loss. The InMemory pair pins the slot-stays-empty
and record-refusal halves symmetric with the file pin's no-leftover-file and no-lost-
snapshot halves.

**Pins, three total:** `FileSnapshotStoreTest#aBoundaryWhoseTermIsNegative_isRejectedWithoutTearingOutTheValidSnapshot`
— a valid `(5,10)` save, then a forged `(-3,10)` save into the same directory: the
refusal throws, the pre-existing valid snapshot STILL LOADS (boundary and bytes
intact), and no unreadable `--3`-named file is left behind.
`InMemorySnapshotStoreTest#aBoundaryWhoseTermIsNegative_isRejectedAndStoresNothing`
— the save throws and the slot remains empty.
`InMemorySnapshotStoreTest#aStoredWhoseBoundaryTermIsNegative_isRejectedByTheRecordItself`
— the `Stored` record itself refuses the forged boundary, pinning the store-independent
layer so the invariant holds even for a store that forgets the axis. All three forge
via the established 6(n) idiom — build a legal `TermIndex(0, index)`, then set the
private final `term` field reflectively to `-3` — the exact shape a deserialized or
construction-bypassed carrier would deliver and the one the constructor forbids.

Isolated: `FileSnapshotStoreTest` **14 green / 0 fail / 0 skip** (13 baseline + 1);
`InMemorySnapshotStoreTest` **9 green / 0 fail / 0 skip** (7 baseline + 2). Full suite
at this point: **379 tests green, 0 failures, 1 pre-existing skip** (+3 over the 6(o)
baseline of 376). The store-input surface and the record now share one answer to
"what is a legitimate `Stored`": non-null, non-empty payload, non-negative term,
non-negative index — the `TermIndex` constructor's contract, enforced at every entry
point the constructor does not reach.
