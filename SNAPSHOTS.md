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
| 6(q) | non-happy-path hardening (fifteenth pass, the <i>type</i> axis of the 6(d)/6(o) "looks like a snapshot but is not one" family on the file surface): 6(d) sealed the NAME axis (unparseable → warn+skip), 6(o) the BODY axis (zero bytes) — but neither gate could express TYPE, because 6(o)'s `hasPayload` is a stat and a directory's stat legitimately reports a positive size (the only question a directory answers with "no" is "am I a regular file?", which the code never asked). A save() writes regular files and nothing else, yet a NON-REGULAR foreign entry named like a snapshot (a directory `raft-snapshot-99-99.snap` in a shared directory) passed every gate that existed: name shape, `parseBoundary`, "has payload". Load path: max-by-boundary "recovered" it (a foreign writer can always name it ABOVE the real snapshot — the 6(o) masking mechanism), `readAllBytes` threw (`Is a directory`), and the one VALID snapshot sitting next to it became unrecoverable — a seed-at-startup node starting unseeded over a log already compaction-truncated to the real boundary. Save path: `deleteSuperseded` visited it as superseded — throwing on a non-EMPTY foreign directory (the failure escapes `save()` as a `RuntimeException`, not the `IllegalStateException` family it wraps, and reaches the caller AFTER the new snapshot is already in place: a partial save reported as one), or silently tearing down an empty foreign object as collateral. Fix: ONE root-cause predicate on both paths — only a regular file is a snapshot — `loadLatest` warns and skips non-regular entries (the 6(o) skip shape; a vanished entry lands on the same guard, still skipped) and `deleteSuperseded` spares them (inert under our naming scheme: never read, never torn out); three pins in `FileSnapshotStoreTest` including the distinguishing valid-snapshot-must-still-load assertion | `stage 6(q)` |
| 6(r) | non-happy-path hardening (sixteenth pass, the 6(g) term axis on the `RaftLogs` intake — the last consumer in the chain behind the sealed wire guards): `compact()` guarded `index < 0` but never `term < 0`, and `commit()` had no guard at all but the index bounds (a null NPEd out of `getIndex()`; a forged `(-1,k)` passes both), sticking into `snapshotBoundary`/`startSeed`/`currentTermIndex` (full install) or `committedTermIndex`, inverting the term-first `TermIndex.compareTo` the committed filter and `rollback()` depend on — committed entries re-served (double-apply) or the pending ones dropped with their acks, and `rollback()` tearing out the committed prefix alongside the uncommitted tail. The axis had looked sealed because the existing negative-<i>index</i> pin was actually pinning the `TermIndex` constructor (it throws inside the pin's own expression) — the same construction-guard illusion that blinded the 6(p) store audit. Both intakes now refuse a null or negative-axis pointer (IAE, the `isHostileWireTermIndex` shape) before ANY state change, with pins carrying the zero-state-change contract and the double-serve/drop discriminator | `stage 6(r)` |
| 6(s) | non-happy-path hardening (seventeenth pass, the `RaftLogs` content intake — the one append path not construction-guarded): all four public append overloads funnel into `appendLogInternal(TermIndex, T)`, and the int overloads reach it only through the `TermIndex` constructor (IAE on both negative axes) — so the raw overload is the one intake a 6(n)/6(p)-shaped construction-bypassed carrier or a null can reach, and it had no guards at all, right next to the 6(r) guards that had just sealed its sibling intakes. Four live shapes, all RED-confirmed pre-fix: **null pointer** NPE'd in the conflict walk (the log is never empty — it is seeded with the epoch); a **forged negative term** could replace nothing (a negative term outranks no existing entry) but is also never "before" one under the term-first `compareTo`, so it fell out of the walk as an ISE "log gap" — the wrong family for a contract refusal; a **forged negative index** tripped `isReplacementOf` against the epoch seed (1 > 0 and -1 <= 0) and the conflict sweep destroyed the ENTIRE log before the forged record lodged below the true tail — state-destroying, with a completed future for index -1 in the caller's hand and the recorded tail a phantom the deque no longer holds; and a **null entry** NPE'd out of the record construction at a new index while silently no-oping (returning the existing record, the entry never read) at an existing one. The gate now refuses the null pointer, the negative-axis pointer, and the null entry at the head of the method, before the iterator touches the deque — one guard covering all four overloads through the funnel, carrying the zero-state-change and next-append-lands-past-the-tail discriminators | `stage 6(s)` |
| 6(t) | non-happy-path hardening (eighteenth pass, the `RaftLogs` read + notification surfaces — the last unguarded public intakes on a class whose mutation intakes 6(r)/6(s) just sealed): `containsStartPoint`, `getLogRange`, and `notifyCommitted` were raw. All RED-confirmed pre-fix: a **null pointer** NPE'd at the `containsStartPoint` `startSeed` equality check or in the `getLogRange` stream filter; a **null transform** NPE'd at the `map` step; **forged negative axes** were silently answered "not contained" or served a range computed off a pointer no `TermIndex` may hold (the range filter indexes the index axis only, so a forged term was ignored and a forged negative index served a wider range than asked); `notifyCommitted(**null)**` NPE'd in the for-each, and a **null element** was skipped with no trace (`instanceof` is simply false) — the caller cannot tell a refusal from a skipped acknowledgement, and the good neighbours completed *first*. Each gate now refuses a null pointer / negative-axis pointer / null transform / null list / null element before touching the list — in `notifyCommitted` validating the WHOLE list before completing ANY future, so a mixed-list refusal is zero-state-change — carrying the zero-state-change, legitimate-range-still-served, and legitimate-ack-still-completes-a-waiting-client discriminators | `stage 6(t)` |
| 6(u) | non-happy-path hardening (nineteenth pass, the pending-request registry — the last raw in-process intake sitting between the sealed wire (6(g)–6(n)) and the sealed log (6(r)–6(t)): `NodeRequests` is the sole request/response correlation point; `NodeConnectionOutboundImpl` is its only caller and funnels BOTH `sendStateRequest` and `receive(StateResponse)` through it, yet neither intake had a single gate. RED-confirmed pre-fix (the pin's first two failures): a **null request** NPE'd synchronously at the `pendingRequests.put(request.getRequestId(), …)` key dereference while the already-scheduled delayed-cancellation task NPE'd a SECOND time, invisibly, 5 s later inside the common pool; a **null response** into `completeRequest` NPE'd at `response.getRequestId()` (the wrong family, both). The remaining shapes are confirmed from the same source walk and guarded in the same pass: a **null response class** was accepted and NPE'd *later*, inside the caller's own future, at `isAssignableFrom(null)` (wrong time, wrong place); a **null requestId** — reachable only via a construction-bypassed carrier (the 6(n)/6(p) shape; the constructors seed the id in-field and can never produce one) — was silently accepted as a legal `HashMap` null key, so two such requests collide on ONE entry and a completion under that key resolved the *WRONG* pending future (talking past each other), and a null-id response carrier fell into the `pending == null` early return and was dropped with no trace — indistinguishable from the legitimate unknown-id discard, and masked in practice by the 5 s cancellation timeout. Both intakes now refuse at the method head in the 6(r)–6(t) IAE family — `getRequestFuture` refusing request / request id / response class before the executor or the map is touched, `completeRequest` refusing response / response id before the map lookup — carrying the legitimate-roundtrip-still-completes-the-waiting-caller and unknown-id-still-a-silent-no-op discriminators | `stage 6(u)` |
| 6(v) | non-happy-path hardening (twentieth pass, the apply/data plane — the last raw in-process intake downstream of the sealed log: `LogsCommitted` — the last unguarded public event carrier — feeds `StateMachineBridge.onEvent`, which replays it into the `PartitionMap` write intakes, and neither side had a single gate. RED-confirmed pre-fix (the pins' first failures, both new test methods): the bridge constructor NPE'd at `eventBus.subscribe` on a null bus with the JDK-enriched message (indistinguishable from a failure deeper in the wiring), and *passed construction* on a null state machine to NPE later, silently, inside an applied write (wrong time, wrong place); `onEvent(null)` fell through into the silent unknown-event-ignore line (indistinguishable from a legitimate discard by another publisher on the shared bus); the carrier held the producer's mutable list by reference, so a nulled element — a shape `List.of` itself would refuse — rode past it, and a forged log (null term index, or null entry: shapes the `RaftLog` constructor never produces) was accepted and NPE'd inside the apply loop — in a mixed `[good, forged]` batch the good log was *applied first* (state changed, applied position moved) and only then the batch died: a half-applied commit; the store intakes bumped `globalVersion` *before* the NPE they threw (every refusal leaked one consumed version — a legitimate SET landed on version **5** where the contract says 1, a follow-up EXPIRE on **5** where it says 2), and `restore` NPE'd mid-restore over a construction-legit `HashMap` with a null key or a null `DataVersion` value. The intakes now refuse at the method head in the 6(r)–6(t) IAE family — constructor arguments refused as named NPEs (`eventBus`/`partitionMap`, the `adopt` `"snapshot"` family; the bus axis pins the message, since the JDK message differed pre-fix), the event refused naming the event while the ignore line narrows to *unknown, non-null* events, the WHOLE batch validated axis by axis (`term index`/`entry`) before anything is applied so a mixed batch is refused as a batch with zero state change and the applied index does not move, the `LogsCommitted` constructor taking `List.copyOf` of a required list (immutable and null-free at the handoff; safe — its only consumer iterates read-only), and the store axes `key` / `input`+`key`+`data` / `input`+`key`+`ttl` / `state` + per-entry `key`+value all refused BEFORE `incrementAndGet()` or any partition mutation — carrying the legitimate-commit-still-applies, refused-restore-leaves-existing-state-untouched-and-restore-then-works, and no-version-leak discriminators | `stage 6(v)` |
| 6(w) | non-happy-path hardening (twenty-first pass, the value-axis layer 6(v) sealed at the boundary but left raw underneath: `DataVersion` — the entry payload of every stored value and of every `Snapshot` entry — held the producer's map in a LIVE `unmodifiableMap` view despite its own javadoc's "a detached copy" (a producer that put/cleared its map after construction still rewrote the installed value, the snapshot entry, and the ALREADY-PUBLISHED `SetDataEvent` payload — the exact mutable-by-reference shape 6(v) closed with `List.copyOf` for `LogsCommitted`); NPE'd a null map with the JDK-enriched "Cannot invoke `Map.getClass()` because `m` is null" (the JDK's own parameter name, indistinguishable from a failure deeper in the wiring); and silently ADMITTED null keys and null values its `Map.copyOf` siblings (`KeyDataInput`/`KeyDataOutput`) refuse at their own heads. And the three `KeyValue` value-writes had no head gate: `setData` STORED the null version first and only then NPE'd at `data.getData()` — leaving the key present but masquerading as never-written (`getData()` serving `Optional.empty`) with the event unpublished — while `updateExpiration`/`restore` stored null with NO exception at all (a snapshot-adopt path under the very guards 6(v) placed upstream). The carrier now holds a true detached copy (`Map.copyOf`) behind the named `"data must not be null"` NPE and the copy's own element NPEs (the `Snapshot`/`LogsCommitted` constructor family), and the three writes refuse a null version at the method head in the 6(v) IAE family before anything is stored, built, or published — carrying the stored-value-survives-a-refused-write, no-view-rewrite-after-construction, and null-map-names-the-axis discriminators | `stage 6(w)` |
| 6(x) | non-happy-path hardening (twenty-second pass, the partition-intake layer directly beneath the 6(v)/6(w) boundary: `KeyMap` — the per-partition key store every `PartitionMap` data intake delegates to, and the silent state-machine path `StateMachineBridge` applies through — had NO gate of its own: `setEntry(null, dv)` rode HashMap's legal-null-key slot straight in, installing a `KeyValue` under a null key AND publishing the `SetDataEvent` naming an unnameable key (a silent install *with* a bus trace — the campaign's worst admission shape); `setEntry(key, null)` installed the shell FIRST and threw only deep (the 6(w) shape one layer up — the shell survived, and a later `deleteEntry` removed it, publishing a `DeleteDataEvent` for a key never written); `getEntry`/`deleteEntry`/`expireEntry` served empty/false on null silently (asymmetric with the IAE family one layer above on the exact same axis); `restore` CLEARED the partition before validating its state map, so a forged null-key/null-value map (construction-legit: `HashMap` admits both, `capture()` would never emit them) silently installed the null-key phantom with the legitimate entries dropped, or threw the version's `DataVersion` IAE after the clear (mutate-then-throw, partial install reached the `capture()` pin); and `PartitionMap`'s two constructor-carrier NPEs carried NO message — the one constructor in the data plane missing the house named-NPE rule (`Snapshot` "entries", `DataVersion` "data", `LogsCommitted` "logs") — and `getPartitionForKey(null)` NPE'd at `key.hashCode()` where every data-plane intake IAEs. The intakes now refuse at the method head in the 6(v) IAE family (`"key must not be null"` in all four `KeyMap` intakes; `"data must not be null"` in `setEntry` before the shell is computed or any event published; `restore` pre-validates the whole state — state/key/state value — before the synchronized clear-install, mirroring the `PartitionMap.restore` template with zero state change on refusal); the constructor carriers now name their dependency (`"eventBus must not be null"`, `"clock must not be null"`); `getPartitionForKey` refuses a null key naming the axis — carrying the legitimate-install-still-works, no-event-published-on-refused-write, shell-not-left-behind (a later `deleteEntry` neither returns true nor publishes), and refused-restore-leaves-the-installed-state-untouched (no clear, no partial install, no phantom) discriminators | `stage 6(x)` |
| 6(y) | non-happy-path hardening (twenty-third pass, the fan-out root no round visited: `EventBus` — the shared bus every sealed subscriber (the `StateMachineBridge` intake, the leader's `AppendLog` subscriber, `Replica`) sits on — had NO gate of its own: `subscribe(null)` silently parked a null element in the `CopyOnWriteArrayList` (null elements are legal there), so the next legitimate publish NPE'd AT DISPATCH (`s.onEvent` on a null subscriber), starving every subscriber after the null one in list order — a bus-level crash triggered by a shape no sealed intake above can emit; `unsubscribe(null)` was a silent no-op indistinguishable from removing a never-subscribed handle; `publish(null)` fanned the null event OUT to the null-tolerant `instanceof`-shaped subscribers (which swallow anything non-matching, including null) and was only refused deep, by the sealed `StateMachineBridge` intake, mid-iteration — aborting delivery to the subscribers after it (6(v) sealed that axis only at the consumer side, the fan-out itself was raw). The three intakes now refuse at the method head in the 6(v) IAE family (`"subscriber must not be null"` for `subscribe` and `unsubscribe`; `"event must not be null"` for `publish` — the same axis name the `StateMachineBridge` intake already speaks on the consumer side, now spoken by the producer BEFORE any subscriber is invoked), with zero state change and zero fan-out on refusal — carrying the no-crash-at-dispatch (a refused subscription leaves both neighboring subscribers still served the legitimate event), installed-subscribers-untouched, zero-fan-out (a refused publish reaches neither of two null-tolerant recorders, where pre-fix the null rode to ALL subscribers silently, or was refused only mid-iteration by the bridge), and legitimate-intake-still-works (subscribe → publish → served exactly once; unsubscribe → publish → not served again) discriminators | `stage 6(y)` |

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
defense-in-depth; see §7); and after stage 6(q): **382 tests green, 0
failures, 1 pre-existing skip** (+3: the <i>type</i> axis of the
"looks like a snapshot but is not one" family — 6(d) sealed NAME and 6(o)
sealed BODY, but neither gate could express TYPE: 6(o)'s `hasPayload` is a
stat and a directory's stat legitimately reports a positive size, so a
NON-REGULAR foreign entry named like a snapshot was "recovered" by
`loadLatest` (the read then throws and the one VALID snapshot beside it is
lost) and visited by `deleteSuperseded` (throwing mid-save after the new
file is in place, or silently torn down as collateral); only a regular file
is a snapshot, now, on both paths; see §7); and after stage 6(r): **383 tests green, 0
failures, 1 pre-existing skip** (+1: the term axis of the 6(g) pointer-family
reaching the `RaftLogs` intake itself — the last consumer behind every sealed
wire guard: `compact()` guarded the null and <i>index</i> half of its boundary
but never the <i>term</i> half, and `commit()` guarded neither (a null NPE'd
out of its first index bound, a forged `(-1,k)` passed both) — so a
construction-bypassed carrier (the 6(n)/6(p) shape) stuck into the seed and
boundary pointers or the committed pointer and, via the term-first
`TermIndex.compareTo`, inverted the committed filter and `rollback()` —
committed entries re-served (double-apply) or pending ones dropped; both
intakes now refuse a null or negative-axis pointer before any state change;
see §7); and after stage 6(s): **385 tests green, 0 failures, 1 pre-existing
skip** (+2: the content intake of the same log — all four append overloads
funnel into `appendLogInternal(TermIndex, T)`, and the int overloads reach it
only through the `TermIndex` constructor, so this raw overload is the one
append path a construction-bypassed carrier or a null can touch — and it had
no guards at all: a null pointer NPE'd in the conflict walk, a forged
negative term fell out as an ISE "log gap", a forged negative index tripped
`isReplacementOf` against the epoch seed and the sweep destroyed the entire
log before the forged record lodged, and a null entry NPE'd at a new index
while silently no-oping at an existing one; the gate now refuses all four
before the deque is touched; see §7); and after stage 6(t): **386 tests green, 0
failures, 1 pre-existing skip** (+1: the read + notification surfaces of the
same log — `containsStartPoint`, `getLogRange`, `notifyCommitted` were the last
unguarded public intakes on the class 6(r)/6(s) sealed on the mutation side,
yet they were raw: a null pointer NPE'd at the start-record equality check or
inside the range stream filter, a null transform NPE'd at the map, forged
negative axes were silently answered "not contained" or served a range computed
off a pointer no `TermIndex` may hold, a null list NPE'd in the for-each, and a
null element was skipped so silently a caller could not tell a refusal from a
skipped acknowledgement (its good neighbours completing first); the gates now
refuse all five hostile shapes before the list is touched, and in
`notifyCommitted` before any future is completed; see §7); and after stage 6(u):
**387 tests green, 0 failures, 1 pre-existing skip** (+1: the pending-request
registry — the last raw in-process intake between the sealed wire and the
sealed log: `NodeConnectionOutboundImpl` is its only caller and funnels
both `sendStateRequest` and `receive(StateResponse)` through `NodeRequests`,
yet neither intake had a gate: a null request NPE'd at
the `put` key dereference (and the already-scheduled cancellation task would NPE
again, invisibly, 5 s later), a null response NPE'd at `getRequestId()`, a null
response class was accepted then NPE'd later inside the caller's own future, and
a null requestId — the construction-bypassed carrier the 6(n)/6(p) passes
established — was silently accepted as a legal `HashMap` null key (a second such
request overwrites the first's pending future, a completion under that key can
resolve the wrong one) or dropped with no trace, indistinguishable from the
legitimate unknown-id discard; both intakes now refuse at the method head in the
same IAE family; see §7); and after stage 6(v): **389 tests green, 0 failures, 1
pre-existing skip** (+2: the apply/data plane — the last raw in-process intake
downstream of the sealed log: `LogsCommitted`, the one unguarded public carrier in
the event family, rode the shared node bus into `StateMachineBridge.onEvent` — whose
constructor (a raw bus, a raw state machine), event intake, and `applyLog` deref had no
gate of their own either — and into the five `PartitionMap` data intakes: a forged
`RaftLog` (a null term index / null entry, shapes the `RaftLog` constructor cannot
produce) NPE'd *inside* `applyLog` only after its legitimate neighbour in a mixed batch
had already been applied and `lastApplied` had moved, a null event fell through into the
silent unknown-event-ignore line, indistinguishable from a legitimate discard, the write
intakes NPE'd at the deref *after* bumping `globalVersion` — one refusal leaked one
global version into the next legitimate write (a SET landing on version 5 instead of 1,
an EXPIRE on 5 instead of 2) — and `restore` NPE'd mid-install over a null-keyed or
null-valued `HashMap`; every intake in the chain now refuses at the method head in the
6(r)–6(t) family, the whole commit batch is validated axis by axis before anything is
applied, and no refusal consumes a version or mutates a partition; see §7); and after stage 6(w): **397 tests green, 0 failures, 1
pre-existing skip** (+8: the value-axis layer 6(v) sealed at the boundary but left raw
underneath: `DataVersion` — the entry payload of every stored value and of every
`Snapshot` entry — held the producer's map in a live `unmodifiableMap` view despite its
own javadoc's "a detached copy" (a producer rewrite still reached the installed value
and the already-published event payload), NPE'd a null map with the JDK's own parameter
name, and admitted null keys and null values its `Map.copyOf` siblings refuse at their
own heads; and `KeyValue`'s three value-writes had no head gate — `setData` stored the
null version first and NPE'd later at `data.getData()` (the key left present but
masquerading as never-written, the event unpublished), `updateExpiration`/`restore`
stored null with no exception at all; the carrier now holds a true copy behind the
named `"data must not be null"` guard and the three writes refuse a null version at the
method head in the 6(v) IAE family before anything is stored, built, or published; see
§7); and after stage 6(x): **407 tests green, 0 failures, 1
pre-existing skip** (+10: the partition-intake layer 6(v) sealed at the `PartitionMap`
boundary but left raw underneath: `KeyMap` — the per-partition key store every data
intake delegates to, and the apply path — silently admitted `setEntry(null, …)`
(installing AND publishing the event under a null key), left the `setEntry(key, null)`
shell for a later `deleteEntry` to remove with a `DeleteDataEvent`, served empty/false
on the other three intakes for null with no exception at all, and `restore` cleared the
partition BEFORE validating (a forged null-key/null-value map dropped the legitimate
entries and installed the phantom, or threw the deep `DataVersion` IAE after the clear);
`PartitionMap`'s two constructor-carrier NPEs were unnamed and `getPartitionForKey(null)`
NPE'd where its intakes IAE; the intakes now refuse at the method head in the 6(v) IAE
family — key/data before any shell install or event publish, whole-state pre-validation
before any clear — and the constructor carriers name their dependencies; see
§7); and after stage 6(y): **411 tests green, 0 failures, 1
pre-existing skip** (+4: the fan-out root no round visited — `EventBus`, the shared bus
every sealed subscriber (`StateMachineBridge`, the leader's `AppendLog` subscriber,
`Replica`) sits on: `subscribe(null)` silently parked a null element
(`CopyOnWriteArrayList` admits them), so the next legitimate publish NPE'd AT DISPATCH,
starving the subscribers after it in list order; `unsubscribe(null)` was a silent
no-op; `publish(null)` fanned the null event out to the null-tolerant
instanceof-shaped subscribers and was refused only deep, by the sealed
`StateMachineBridge` intake, mid-iteration, aborting delivery past it; the three
intakes now refuse at the method head in the 6(v) IAE family — `subscriber must not be
null` in subscribe/unsubscribe, `event must not be null` in publish before any
subscriber is invoked; see §7).
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

**Non-happy-path hardening pass, fifteenth round (2026-11-24, stage 6(q))** — the
<i>type</i> axis of the "looks like a snapshot but is not one" family the 6(d) and 6(o)
passes sealed on the file surface, open in exactly the one member neither fix ever
considered: a NON-REGULAR entry bearing a snapshot name. The audit that closed 6(o)
ruled the family complete because its two gates answered every question a <i>file</i>
can pose: the 6(d) gate asks of the NAME ("does it name a boundary?"), the 6(o) gate
asks of the BODY ("does it carry bytes?"). But `hasPayload` is itself a `Files.size`
stat, and a directory's stat legally reports a positive size (40 bytes on this box,
verified), so a stat cannot express the one question a directory answers with "no" —
"am I a regular file?" — and the code had never once asked it. Meanwhile the store's
own contract is explicit about that member: `save()` writes a same-directory regular
temp file and renames it, and never anything else. So a non-regular entry with a
snapshot name can only be foreign, and it was the one shape every gate waved through:

- **Load path.** A directory `raft-snapshot-99-99.snap` in the store's directory passed
  everything: name shape, `parseBoundary` (a name is a name and does not know it names
  a directory), and `hasPayload` (stat 40 > 0). It then won the max-by-boundary
  selection — and a foreign writer can always name the impostor for a boundary ABOVE
  the real snapshot — which is exactly the 6(o) masking mechanism, this time against a
  type both earlier fixes could *read* but never *considered*. Selection returns the
  directory, `Files.readAllBytes` throws `Is a directory`, `loadLatest` surfaces an
  `IllegalStateException` — and everything in the directory but the directory's own
  bytes was a valid snapshot now UNREACHABLE through the store's only read path. A
  node seeding at startup starts unseeded over a log already compacted to the real
  boundary: the data loss 6(o) forbade, re-emerging through the type axis, and in the
  strictly worse posture — in 6(o) the real snapshot at least still <i>existed</i>.
- **Save path.** `deleteSuperseded` filtered by name shape only and visited every
  non-<i>keep</i> entry as "a superseded snapshot": on a NON-EMPTY foreign directory
  (the realistic foreign-writer shape), `Files.deleteIfExists` throws
  `DirectoryNotEmptyException`, and the `RuntimeException` wrapping it escapes `save`'s
  `catch (IOException)` unconverted — reaching the caller AFTER the new snapshot was
  already moved into place, in an exception family no other `save` failure uses: a
  partial save reported as one, with whether the earlier regular snapshots were already
  gone (a stream-order question) left to chance. On an EMPTY foreign directory, the
  mirror: `deleteIfExists` quietly succeeds and the store tears down a foreign object
  it did not create — the collateral the 6(d) read-only contract refuses on the load
  side.

The fix is one root-cause predicate, applied on both paths — **only a regular file is a
snapshot**:

- `loadLatest`'s collect pass refuses `Files.isRegularFile(p)` with a warning, in the
  exact shape of the 6(o) empty-payload skip: warn, skip, leave in place. A vanished or
  unstat'able entry lands on this same guard (`isRegularFile` is false for both) and is
  still skipped — the 6(o) "cannot stat" branch remains as defense-in-depth, no longer
  reached first.
- `deleteSuperseded` filters to regular files: a non-regular entry with a
  snapshot-shaped name is not a superseded snapshot and is never touched. Load already
  skips the same shape (6(d)/6(o), now 6(q)), so the pair is consistent: an entry that
  is not a regular file is inert under our naming scheme — never selected, never read,
  never torn out.

Why the type check is a root-cause guard and not one more ad-hoc skip: the store's
entire model is "a snapshot is a FILE named for a boundary, with a payload body." The
name axis and the body axis were sealed with contract-shaped gates; the FILE-ness was
the one word of that sentence the code never expressed, and `hasPayload` stat'ing a
directory to 40 bytes is the tell — a gate that tests "does it carry bytes" passes a
directory, because directories do carry stat bytes. Only `isRegularFile` asks the
question the contract depends on.

**Pins, three, all in `FileSnapshotStoreTest`:**
`aDirectoryNamedLikeASnapshot_isInvisibleToLoadLatest` — the distinguishing pin: a
valid `(5,10)` save, then a directory `raft-snapshot-99-99.snap` named for a HIGHER
boundary (the masking shape): `loadLatest` must still return `(5,10)` with its payload
intact, and the directory left in place. Pre-fix, this threw `IllegalStateException`;
asserting the VALID snapshot's survival is what distinguishes a skip from "the load
worked by luck" — the same distinguishing discipline as the 6(o) hollow-file pin and
the 6(p) no-torn-out pin.
`aDirectoryWhoseOnlySnapshotEntryIsNonRegular_loadsAsEmpty` — with nothing to fall
back to a directory-only "store" reads as "no snapshot": the same
`Optional.empty()` a fresh store gives (the 6(o) hollow-only pin's twin on the type
axis), with the entry left in place. Pre-fix: `IllegalStateException`.
`save_leavesANonRegularForeignEntryAlone_evenAtASupersededBoundary` — a NON-EMPTY
foreign directory `raft-snapshot-3-7.snap` at a LOWER boundary: after saving `(9,20)`,
`save` must COMPLETE (pre-fix it threw `RuntimeException` with the new file already in
place — the partial save), the new snapshot loads, the superseded REGULAR `(5,10)`
snapshot is still deleted (retention preserved for real snapshots), and the foreign
directory and its contents are intact.

Isolated: `FileSnapshotStoreTest` **17 green / 0 fail / 0 skip** (14 baseline + 3).
Full suite at this point: **382 tests green, 0 failures, 1 pre-existing skip** (+3
over the 6(p) baseline of 379). The family — name (6(d)), body (6(o)), type (6(q)) —
is now closed on the file surface: an entry is a snapshot iff it is a regular file
whose name names a boundary and whose body is non-empty, and every other shape under
our naming scheme is warned about on load, spared on save, and left in place on both.

**Non-happy-path hardening pass, sixteenth round (2026-11-24, stage 6(r))** — the
term axis of the 6(g) pointer family reaching the `RaftLogs` intake itself — the last
consumer in the chain behind the wire guard sealed by 6(g)–6(n) and the store argument
surface sealed by 6(o)–6(p). A consumer feeding the log directly from inside the process
bypasses every one of those guards at once, and the log's two intake points both carried
a guard that *looked* complete and was half-sealed:

- `compact(boundary, payload)` guarded `boundary == null` and `boundary.getIndex() < 0`
  — but never `boundary.getTerm() < 0`. The axis had looked sealed because the shape is
  unbuildable by construction: the `TermIndex` constructor throws IAE on either negative
  axis. That is exactly the 6(n)/6(p) mechanism — and the tell is that the pre-existing
  negative-<i>index</i> pin in `testCompactGuards` (`compact(new TermIndex(1, -1L), …)`)
  was pinning the CONSTRUCTOR: the IAE it asserted was thrown inside the pin's own
  expression, so the guard it claimed to pin had never actually been exercised. A
  construction-bypassed carrier (deserialized or reflected — the 6(n) pin idiom) carries
  `(-1, k)` past both checks.
- `commit(termIndex)` guarded neither. `termIndex == null` NPE'd out of the first index
  bound (`Cannot invoke "TermIndex.getIndex()"`); a forged negative term passed both
  index comparisons, because they only test the index.

Pre-fix damage (both RED-confirmed during the pin cycle):

- `compact(forgeTerm(-1, 2L))` was ACCEPTED — the log line proves it: `Compacted log to
  boundary TermIndex{term=-1, index=2} (dropped 2 record(s), 0 kept)`. Both real records
  dropped and the forged boundary reseeding the deque: start point, `snapshotBoundary`,
  and (full install) the recorded tail all become `(-1, 2)` — a term no `TermIndex` may
  legally hold, served back through `getStartTermIndex()` onto the wire, where the
  6(g)-guarded install path refuses it forever, and into the term-first `compareTo`
  ordering `commit` and `rollback` depend on.
- `commit(null)` surfaced as an NPE — the wrong exception family for a contract refusal.
  `commit(forgeTerm(-1, 1L))` was accepted silently into `committedTermIndex`. The
  committed filter is `r.compareTo(prevCommit) > 0 && r.compareTo(newCommit) <= 0` — both
  term-first — so with the committed pointer at term `−1` the next legitimate
  `commit((1, 2))` RE-SERVES an already-committed entry (a double-apply against the
  apply-then-acknowledge ordering the method exists to protect), and `rollback()`
  (`compareTo(committed) > 0`) reports committed entries as uncommitted.

The fix is the 6(o)/6(p) guard shape on the two last intakes — refusal BEFORE any state
change, on both axes:

- `compact`: the existing index check extended to `getTerm() < 0 || getIndex() < 0`
  (after the null check; both IAE, the `isHostileWireTermIndex` shape the wire guards
  already use).
- `commit`: a new `termIndex == null` → IAE and `getTerm() < 0 || getIndex() < 0` → IAE,
  placed before the index bounds — the null no longer NPEs out of the bound reads and a
  forged term no longer slips past index-only comparisons.

**Pins (both RED-confirmed before the fix, then GREEN), in `RaftLogsTest`:**
`testCompactGuards`, extended — `compact(forgeTerm(-1, 2L), "hostile")` throws IAE and
the refusal is ZERO-STATE-CHANGE (the start point is still `EPOCH`,
`containsStartPoint((1, 1))` is true, tail index still 2): pre-fix the same call was
accepted with both records dropped.
`testCommitGuards`, new — `commit(null)` throws IAE (pre-fix: NPE),
`commit(forgeTerm(-1, 1L))` throws IAE (pre-fix: accepted), the committed pointer is
untouched at `(1, 1)`, and then a legitimate `commit((1, 2))` returns EXACTLY `[b]`:
the double-serve/drop discriminator — had the forged pointer stuck, that same commit
would have re-served the already-committed `a` (double-apply), asserting `[b]` precisely
is what distinguishes a real refusal from a lucky filter.
Both forge through the established 6(n)/6(p) idiom — `new TermIndex(0, index)` with the
private final `term` field set reflectively to `-1` (`forgeTerm` helper, now shared by
`RaftLogsTest`) — the exact shape a deserialized or otherwise construction-bypassed
carrier would deliver and the one the constructor forbids.

Isolated: `RaftLogsTest` **10 green / 0 fail / 0 skip** (9 baseline + the new
`testCommitGuards` method; the `testCompactGuards` pin extended an existing method, no
delta). Full suite at this point: **383 tests green, 0 failures, 1 pre-existing skip**
(+1 over the 6(q) baseline of 382). The pointer family — wire (6(g)–6(n)), store
(6(o)–6(p)), log (6(r)) — now has one answer at every in-process entry point: a pointer
is legitimate iff it is non-null and both axes non-negative — the `TermIndex`
constructor's contract, enforced at every API the constructor does not reach.

**Non-happy-path hardening pass, seventeenth round (2026-11-24, stage 6(s))** — the
CONTENT intake of the same log its 6(r) sibling sealed: `appendLog` / `appendFutureLog`.
The pointer half of 6(r) closed `compact` and `commit`; the fourth intake of the class —
the one the whole log exists to append through — sat directly below those two guards with
none of its own, and its "sealed" appearance was built from the 6(n)/6(p) construction
illusion in the strongest form: every guard the reader could name belonged to the
`TermIndex` constructor, but only the int overloads route through it.

The surface. All four public append overloads funnel into the private
`appendLogInternal(TermIndex, T)`: the int overloads `appendLog(int, int, T)` and
`appendFutureLog(int, int, T)` construct their pointer with `new TermIndex(term, index)`
— IAE on either negative axis, NPE on null args, construction-sealed — while the
`TermIndex`-typed overloads `appendLog(TermIndex, T)` (L97) and
`appendFutureLog(TermIndex, T)` (L89) hand the pointer to `appendLogInternal` raw. So
this one private method is the single intake a construction-bypassed carrier (the
6(n)/6(p) shape, on either axis) or a null can reach from inside the process — exactly
the gap 6(r) had noted its sibling guards were designed to prevent.

The pre-fix behaviors (all RED-confirmed by the pins before the fix):
- **null pointer**: NPE inside the conflict walk at `isReplacementOf` — the deque is
  never empty (the constructor seeds the epoch entry), so the dereference is
  deterministic. Zero state change, wrong family (NPE, not IAE).
- **forged negative term** `(-1, k)`: `isReplacementOf` can never fire (nothing outranks
  a negative term) AND the term-first `compareTo` can never rank it "before" an entry —
  it falls out of the walk as ISE `"Unexpected log gap!"`. Zero state change, wrong
  family (ISE, not IAE).
- **forged negative index** `(1, -1)` — the worst shape: `isReplacementOf` fires against
  the epoch seed (`1 > 0 && -1 <= 0`), so the conflict walk **sweeps the entire log**,
  lodges the forged record, returns a *completed future* for index `-1`, and the next
  computed append continues from `currentTail().getIndex() + 1 = 0`. State destroyed,
  caller handed success.
- **null entry**: at a NEW index, NPE out of the `RaftLog` constructor's
  `requireNonNull(entry)`; at an EXISTING pointer, the walk matches and returns the
  existing record without ever reading the raw entry — a silent no-op. One intake, two
  behaviors; a uniform gate is the only answer.

**The fix, in `RaftLogs`**, at the head of `appendLogInternal(TermIndex termIndex, T
rawLog)` — before the iterator touches the deque, so one guard covers all four public
overloads through the funnel, in the `isHostileWireTermIndex` / 6(r) family:
`termIndex == null` → IAE `"append termIndex must not be null"`; `getTerm() < 0 ||
getIndex() < 0` → IAE `"append termIndex must not be negative: …"`; `rawLog == null` →
IAE `"append entry must not be null (pointer …)"`. Deliberately NOT a protocol-level
refusal of any higher-term-at-lower-index shape — that is Raft's legitimate conflict
truncation (`isReplacementOf`) and the guard must never eat it.

**Pins (both RED-confirmed before the fix, then GREEN), in `RaftLogsTest`:**
`testAppendGuards`, new (modeled on `testCommitGuards`) — `appendLog(null, "hostile")`
throws IAE (pre-fix: NPE@L116), `appendFutureLog(null, "hostile")` throws IAE (mirror
pin, holds the funnel), `appendLog(forgeTerm(-1, 1L), "hostile")` throws IAE (pre-fix:
ISE "log gap"), and `appendLog(new TermIndex(1, 1L), null)` (existing pointer — the
silent-no-op shape) and `appendLog(new TermIndex(1, 9L), null)` (new pointer — the NPE
shape) both throw IAE; the refusal is zero-state-change (both records intact, tail at 2,
full range `["a","b"]`) and then `appendLog(1, "c")` at (1,3) still resolves — the
legitimate-append-lands-past-tail discriminator. `testAppendGuardsRefuseForgedNegativeIndex`,
separate, on its own fresh `RaftLogs` instance (isolation: the destructive shape must not
leak state into the other pin) — `appendLog(forgeIndex(1, -1L), "hostile")` throws IAE
(pre-fix: nothing thrown, log destroyed), then the zero-state-change assertions (`[a, b]`
intact, tail at 2, legitimate (1,3) append lands). The new `forgeIndex` helper mirrors
`forgeTerm` — reflectively set the `index` field of a freshly-constructed `TermIndex` —
completing the two-axis forging idiom of 6(n)/6(p)/6(r) in `RaftLogsTest`.

Isolated: `RaftLogsTest` **12 green / 0 fail / 0 skip** (10 baseline + the two new
methods). Full suite at this point: **385 tests green, 0 failures, 1 pre-existing skip**
(+2 over the 6(r) baseline of 383). The pointer+content family of the `RaftLogs` intake —
compact (6(r) term+index), commit (6(r) term+index), append (6(s) pointer + entry) — now
refuses every hostile shape at every public surface that takes one, before any mutation of
the deque.

**Non-happy-path hardening pass, eighteenth round (2026-11-24, stage 6(t))** — the READ
and NOTIFICATION surfaces of the same log 6(r)/6(s) sealed on the mutation side:
`containsStartPoint`, `getLogRange`, and `notifyCommitted`. The pointer family has one
answer at every in-process entry point that took a pointer; these three are the last
public intakes on the class that did not — and, unlike the 6(s) intake, they had not even
the construction-guard illusion to blind an audit: nothing here constructs a `TermIndex`,
so there was no guard-shaped artifact at all to find.

The surfaces. `containsStartPoint(TermIndex index)` reads its pointer at the FIRST
start-record equality check — the start record always exists — so a null pointer is a
deterministic NPE, not a conditional one. `getLogRange(TermIndex, int, Function)`
dereferences `startTermIndex.getIndex()` inside the stream filter and applies
`fnTransform` at the `map`; the two-arg overload funnels in with `Function.identity()`,
so one gate at the three-arg version holds both overloads. `notifyCommitted(List)`
iterates the raw list and completes each `CompletableRaftLog`'s future as it goes — a
null list NPEs in the for-each, and a null element simply fails the `instanceof` test.

The pre-fix behaviors (all RED-confirmed by the pin before the fix):
- **null pointer** to either reader: NPE — at `index.equals(startSeed.getTermIndex())`
  in `containsStartPoint`, at `startTermIndex.getIndex()` inside the `getLogRange`
  filter. Zero state change, wrong family (NPE, not IAE).
- **forged negative axis**: silently *answered* — "not contained" in
  `containsStartPoint` (the axes never match and the negative index compares "before"
  the seed), and in `getLogRange` a range computed off a pointer no `TermIndex` may
  hold: the filter indexes the index axis only, so a forged term is ignored and a forged
  *negative index* serves a wider range than was asked. Zero state change, silently
  accepted — the most dangerous of the three behaviors here, because it masquerades as
  a plausible result.
- **null transform** to `getLogRange`: NPE at the `map` step. Zero state change, wrong
  family.
- **null list** to `notifyCommitted`: NPE in the for-each, wrong family. **Null
  element**: skipped with no trace — `null instanceof CompletableRaftLog` is simply
  false — and critically the good neighbours in the same list complete *before* the bad
  one is reached: a caller passing `[a, null]` cannot tell a refusal from a skipped
  acknowledgement, and had already mutated observable state (acknowledgements are the one
  side effect on the read surface).

The fix is the 6(r)/6(s) guard shape on all three, at the head of each public method (the
`isHostileWireTermIndex` / IAE family): `containsStartPoint` now refuses `index == null`
and `getTerm() < 0 || getIndex() < 0` before the first equality check; `getLogRange`
refuses a null start pointer, a negative-axis start pointer, and `fnTransform == null` —
one gate covering both overloads through the funnel, so the two-arg path inherits the
same contract; `notifyCommitted` refuses `committedLogs == null` and a null element,
validating the WHOLE list before completing ANY future — the mixed list `[a, null]` is
now refused with `a`'s future still pending, which is what "zero state change" means on a
surface whose only state change is completing futures.

**Pin (RED-confirmed before the fix, then GREEN), in `RaftLogsTest`:**
`testReadNotificationGuards`, new (modeled on the 6(r)/6(s) guard methods) —
`containsStartPoint(null)` IAE (pre-fix: NPE),
`containsStartPoint(forgeTerm(-1, 1L))` and `containsStartPoint(forgeIndex(-1L))` IAE
(pre-fix: silently "not contained"), while legitimate `(1, 1)`/`(1, 2)` still resolve and
an unknown `(9, 9)` still answers false; `getLogRange(null, 2)`,
`getLogRange(null, 2, …)` (both overloads), `getLogRange(forgeTerm(-1, 1L), 10, …)`,
`getLogRange(forgeIndex(-1L), 10, …)` (the wider-range shape), and
`getLogRange(new TermIndex(0, 0L), 2, null)` (the transform axis) all IAE (pre-fix: NPE
/ NPE / silently served `[b]` / silently served `["a","b"]` / NPE), while the legitimate
`getLogRange(new TermIndex(0, 0L), 2, RaftLog::getEntry)` still serves exactly
`["a","b"]`; `notifyCommitted(null)` IAE (pre-fix: NPE),
`notifyCommitted([null])` IAE (pre-fix: silently skipped), and `notifyCommitted([a, null])`
— the mixed-list shape — IAE (pre-fix: `a`'s future completed, `null` skipped, nothing
thrown: the refusal indistinguishable from a skip), while a legitimate acknowledgement
still completes a waiting client (`appendFutureLog(1, "c")` → `commit((1, 3))` →
`notifyCommitted(newlyCommitted)` → the future resolves to "c"). The
zero-state-change discriminator closes it: both real records intact, the tail at 3, and
the full range `["a","b","c"]` still served after every refusal.

Isolated: `RaftLogsTest` **13 green / 0 fail / 0 skip** (12 baseline + the new method).
Full suite at this point: **386 tests green, 0 failures, 1 pre-existing skip**
(+1 over the 6(s) baseline of 385). The `RaftLogs` surface is now closed end to end:
compact (6r), commit (6r), append (6s), contains (6t), range (6t), notify (6t) — every
public intake refuses a hostile carrier (null pointer, negative-axis pointer, null entry,
null transform, null list, null element) at the head of its method, before the deque,
the stream, or any future is touched. The pointer+carrier family the house has been
closing since 6(g) — wire (6(g)–6(n)), store (6(o)–6(p)), log (6(r)–6(s) + this pass) —
now enforces one contract at every in-process entry point: a pointer is legitimate iff
it is non-null and both axes non-negative, and a list is legitimate iff it holds no nulls.

**Non-happy-path hardening pass, nineteenth round (2026-11-24, stage 6(u))** — the
PENDING-REQUEST REGISTRY: `NodeRequests`. 6(g)–6(n) sealed the wire, 6(o)–6(p) the store,
6(r)–6(t) the log — but between wire and behaviour there was one raw intake none of those
passes touched, because it carries no wire shape at all: the in-process correlation registry
that `NodeConnectionOutboundImpl` — its only caller — funnels `sendStateRequest` into
(`getRequestFuture`) and `receive(StateResponse)` through (`completeRequest`). Everything
around it is guarded; the registry itself was the hop where a hostile carrier could still
walk in and corrupt the correlation state of the very requests the sealed intakes had just
validated.

The surface. `getRequestFuture(request, responseClass)` schedules a delayed executor
(5 s) that will cancel the pending future, builds the `thenApply` chain that validates the
arriving response against `responseClass`, and registers the pending future keyed by
`request.getRequestId()` before returning it to the caller. `completeRequest(response)`
looks up the pending future by `response.getRequestId()` and either silently returns
(`pending == null`) or completes it with the arrived response. The registry's only state is
the `pendingRequests` map and the futures it holds.

Neither intake had a single gate. The pre-fix behaviors (the first two RED-confirmed by the
pin before the fix; the remainder confirmed from the source walk, guarded in the same pass):
- **null request** (RED-confirmed): NPE, synchronously, at the `pendingRequests.put(request.getRequestId(), …)`
  key dereference — and, in parallel, the delayed-cancellation task was already scheduled
  and would NPE a **second** time, invisibly, 5 s later inside the common pool: the wrong
  family, plus a deferred failure the caller can never see.
- **null response** (RED-confirmed): NPE at `response.getRequestId()` — wrong family.
- **null response class**: the call silently succeeds; the NPE arrives **deferred**, inside
  the *caller's own future*, when the response arrives (`isAssignableFrom(null)`): an error
  that surfaces at the wrong time, in the wrong place, as a failure of a future the caller
  is holding.
- **null requestId, request side** (the 6(n)/6(p) construction-bypassed carrier — the
  constructors seed the id in a field initializer and can never produce one; only a
  deserialized stream or a subclass override can): `HashMap` accepts `null` **as a key**, so
  the call silently succeeds, the entry is registered, and *two* such requests collide on
  the ONE null-keyed entry — the second registration overwrites the first's pending future,
  and a completion under that key resolves whichever entry happens to be there: the WRONG
  pending future (talking past each other), or a 5 s cancellation task cancelling a future
  whose caller is still waiting.
- **null-id response carrier**: `pendingRequests.get(null)` silently early-returns —
  dropped with no trace, indistinguishable from the *legitimate* unknown-id discard (that
  axis's existing contract is "unknown completions are ignored", and this is the hostile
  version of it), masked in practice by the 5 s cancellation timeout the caller observes.

The fix is the 6(r)–6(t) guard shape, at the head of BOTH intakes (the
`isHostileWireTermIndex` / IAE family): `getRequestFuture` refuses `request == null`,
`request.getRequestId() == null`, and `responseClass == null` BEFORE the executor is
scheduled or the map is touched, so the null-request shape no longer even leaves a pending
task; `completeRequest` refuses `response == null` and `response.getRequestId() == null`
before the map lookup. The house contract extends by exactly one axis, continuing the
6(t) closing sentence: a carrier is legitimate iff it is non-null, and a request/response
pair is legitimate iff the request, the response, the expected class, and the correlation
id that binds them are all non-null.

**Pin (RED-confirmed before the fix, then GREEN), `NodeRequestsTest` new:**
`testPendingRegistryGuards` — `getRequestFuture(null, StateResponse.class)` IAE (pre-fix:
NPE at the `put` key dereference), `completeRequest(null)` IAE (pre-fix: NPE at
`getRequestId()`), `getRequestFuture(validRequest, null)` IAE (pre-fix: silent success,
NPE deferred into the caller's future), and `getRequestFuture(NullIdRequest,
StateResponse.class)` / `completeRequest(NullIdResponse)` IAE (pre-fix: silent
acceptance — null is a legal `HashMap` key). The null-id shapes are built with private
static subclasses overriding `getRequestId()` to return null — the only in-repo way to
produce a construction-bypassed carrier (the 6(n)/6(p) shape). Discriminators: a
**legitimate round trip** — `getRequestFuture(liveRequest, StateResponse.class)` →
`completeRequest(liveResponse)` → the caller's future resolves to exactly the awaited
response within 1 s — still works; and a response with a **legitimate-but-unknown** id
still lands on the silent `pending == null` no-op (the existing unknown-id contract is
preserved; only hostile carriers are now refused).

Isolated: `NodeRequestsTest` **1 green / 0 fail / 0 skip** (new test class).
Full suite at this point: **387 tests green, 0 failures, 1 pre-existing skip**
(+1 over the 6(t) baseline of 386). The wire→behaviour funnel is now closed at every
in-process intake: wire (6(g)–6(n)), store (6(o)–6(p)), log (6(r)–6(t)), and the
request/response bus (this pass). The pointer+carrier family the house has been closing
since 6(g) enforces one contract at every in-process entry point: a pointer is legitimate
iff non-null with both axes non-negative, a list iff it holds no nulls, and a
request/response pair iff the carrier and its correlation id are non-null.

**Non-happy-path hardening pass, twentieth round (2026-11-24, stage 6(v))** — the
apply/data plane: 6(g)–6(n) sealed the wire, 6(o)–6(p) the store, 6(r)–6(t) the log, and
6(u) the request/response registry — but between the sealed log and the state the log
replays into there was still one chain with no gate on any hop, because every hop is
in-process and carries no wire shape: `LogsCommitted`, the one public event carrier
with no element gate, rode the shared node bus straight into
`StateMachineBridge.onEvent`, whose `applyLog` fed the five `PartitionMap` data intakes —
and the bridge's own constructor took a raw bus and a raw state machine. A
construction-bypassed `RaftLog` (a null term index / a null entry — shapes the `RaftLog`
constructor cannot produce, the 6(n)/6(p) shape) or a plain null could walk that whole
chain in and reach `setData`, `deleteData`, `expireData`, and `restore`, the very
surfaces this house has been sealing intakes on since 6(g).

The surface. `StateMachineBridge(EventBus, PartitionMap)` wired `eventBus.subscribe(this)`
with no check on either dependency. `onEvent(Object)` branched on event type only — a
null sailed through both `instanceof` guards into the unknown-event-ignore line, the same
line a *legitimate* discard by another publisher lands on. `LogsCommitted` stored the
producer's mutable list **by reference** with no element check — the one carrier a
nulled element (a shape `List.of` would itself refuse) could ride on, and it could still
be mutated by its producer after publish. `applyLog` dereferenced
`getTermIndex().getIndex()` first, then `getEntry()`. And the five `PartitionMap`
intakes — `getData`/`setData`/`deleteData`/`expireData`/`restore` — dereferenced their
key/input/data/ttl/state axes at the partition walk, the three write intakes bumping
`globalVersion` **before** the dereference that threw, and `restore` writing partitions
as it walked.

The pre-fix behaviours (all five RED-confirmed by the pin before the fix):
- **null bus into the bridge constructor**: NPE at the `eventBus.subscribe` call, with
  the JDK's enriched message — the wrong family, indistinguishable from a wiring failure;
  **null state machine**: no failure at construction at all — deferred and silent, an NPE
  inside an applied write.
- **null event on the shared bus**: silent — the ignore line is the SAME path a
  legitimate discard by another publisher takes; no error, no family, no trace.
- **a nulled `LogsCommitted` element**: rode the chain by reference into the apply loop,
  NPE'd there — the wrong family, mid-apply, *after* a legitimate neighbour in the same
  batch might already have been applied.
- **a forged `RaftLog`** (null term index → NPE at `termIndex.getIndex()`; null entry →
  NPE at the `instanceof` dispatch): worst in a **mixed `[legitimate, forged]` batch** —
  the legitimate log **applied first** — state changed, `lastApplied` moved — and only
  then the batch died on the forged element: a *half-applied commit* at the very surface
  the sealed log is meant to make atomic.
- **null axes into the `PartitionMap` intakes** (null key / input / data / ttl / state /
  state value — raw and 6(n)/6(p)-forged): NPE at the dereference — and, for the write
  intakes, **after** `globalVersion.incrementAndGet()`: one refusal consumed one global
  version, so the next legitimate write landed one higher than its caller expected (a
  SET on version 5 where the sequence was 1; an EXPIRE on 5 where it should have been 2);
  and `restore` NPE'd **mid-install** over a `HashMap` carrying a null key or a null
  `DataVersion` — a shape neither `Map.of` nor a captured state would ever produce —
  with partitions already half-replaced.

The fix is the 6(r)–6(t) gate family at the head of every intake in the chain, in order:
the constructor names both refused dependencies (`"eventBus"`, `"partitionMap"`) before
any wiring; `onEvent` refuses a null event before the type branches, so the ignore line
is reachable only by non-null unknown events — the legitimate case; `LogsCommitted` now
stores an **immutable copy** (`List.copyOf`) that rejects a null list (`"logs"`) and a
nulled element at construction, so the carrier itself can no longer hold one or be
mutated by its producer after publish; the `onEvent` apply path validates the **whole
batch** — "log must not be null", "term index must not be null", "entry must not be
null" per element, **before** the apply loop (the 6(t) `notifyCommitted` precedent) — so
one forged element refuses the entire batch with **zero state change** (`lastApplied`
untouched); and all five `PartitionMap` intakes refuse their null axes at the method
head in the 6(r)–6(t) IAE family ("key must not be null", "input must not be null",
"data must not be null", "ttl must not be null", "state must not be null", "state value
must not be null") — **before** any `incrementAndGet()` and before any partition
mutation, with `restore` validating every entry (key and value) before its install loop.

**Pin (RED-confirmed before the fix, then GREEN), `StateMachineBridgeTest` and
`PartitionMapTest` extended:** new `testApplyIntakeGuards` (BridgeTest, 22) — the
constructor NPEs on each null dependency, **naming it** (`"eventBus"`,
`"partitionMap"`); `new LogsCommitted(null)` and a list carrying a nulled element NPE
(`"logs"`); `onEvent(null)` IAE `"event must not be null"` (was: the silent ignore
line); a forged null-term-index log and a forged null-entry log are refused at the
*publisher's* `publish` call site — the bus is synchronous, so the IAE surfaces there,
not in the background — with `"term index must not be null"` / `"entry must not be
null"` (was: NPE mid-apply); the **mixed `[legitimate, forged]` batch** is refused as a
batch — `getData(legitimate-key)` still empty, `getLastAppliedIndex()` still `0` — and
then the discriminator, a **legitimate commit still applies** (`getLastAppliedIndex()`
1), so the refused intake remains fully usable; new `testStoreIntakeGuards` (
PartitionMapTest, 14) — the null axes of `getData`/`setData`/`deleteData` (raw and
6(n)/6(p)-forged: null key / null PUT data) and of `expireData` (null input, null key,
null ttl) and of `restore` (null state, null key, null state value) all IAE with the
axis named, plus the **version-leak discriminators** the pre-fix code failed: after the
write refusals a legitimate SET still lands on global version **1** (pre-fix: **5**),
and after the EXPIRE refusals SET → legitimate EXPIRE leaves its entry on version **2**
(pre-fix: **5**); on the `restore` path the refusals leave the existing partition
byte-identical — no partial install — and a **legitimate** restore then installs its
entries at their restored version (v7), drops the keys absent from it (the "existing"
state is gone), fast-forwards the counter, and the NEXT local write lands on v8 — the
intake is usable after refusing, and the versions stay strictly increasing.

Isolated: `StateMachineBridgeTest` **22 green** and `PartitionMapTest` **14 green** —
**36 green / 0 fail / 0 skip**.
Full suite at this point: **389 tests green, 0 failures, 1 pre-existing skip**
(+2 over the 6(u) baseline of 387). The pointer+carrier family the house has been
closing since 6(g) now enforces its one contract at every in-process entry point — wire
(6(g)–6(n)), store (6(o)–6(p)), log (6(r)–6(t)), request/response registry (6(u)), and
this pass, the apply/data plane (6(v)) — and the last raw intake in the chain, the one
that turns the sealed log into state, now refuses the shapes the house's own sealed
constructors can never produce: at the head of the bridge, at the head of the carrier,
and at the head of every store intake, before a version is consumed or a partition is
touched.

**Non-happy-path hardening pass, twenty-first round (2026-11-24, stage 6(w))** — the
value-axis layer: 6(v) sealed the apply/data plane *at the boundary* — every
`PartitionMap` intake now refuses its null axes before a version is consumed or a
partition is touched — but the values those intakes store sit on two carriers with no
gate of their own: `DataVersion`, the entry payload of every stored value **and** of
every `Snapshot` entry (the 6(v) `SnapshotInstalled` foreign-payload pin already shows
hostile payload shapes arriving there), and `KeyValue`, the key into which the
value-writes store the latest `DataVersion`. A construction-bypassed or deserialized
carrier — the 6(n)/6(p) shape established at every intake above — reaches both:

- **`DataVersion` constructor axes** (null map / null key / null value): a null map
  NPE'd at `Collections.unmodifiableMap(null)` with the JDK-enriched "Cannot invoke
  `Map.getClass()` because `m` is null" — the JDK's *own* parameter name,
  indistinguishable from a failure deeper in the wiring; null keys and null values were
  **silently admitted** (a legal `HashMap` shape); and the whole map was held *by
  reference* — the constructor's own javadoc says "the data map is a **detached
  copy**", but it was a live `unmodifiableMap` view, so a producer that rewrote or
  cleared its map after construction still reached through it into the installed value,
  the snapshot entry, and the `SetDataEvent` payload already published — the exact
  mutable-by-reference shape 6(v) closed with `List.copyOf` for `LogsCommitted`, and an
  element shape the `Map.copyOf` siblings (`KeyDataInput`/`KeyDataOutput`) already
  refuse at their own heads.
- **null `DataVersion` into the three `KeyValue` value-writes** (`setData` /
  `updateExpiration` / `restore`): `setData` **stored** the null version first and only
  then NPE'd at `data.getData()` — the refusal left the key present but masquerading as
  never-written (`getData()` serving `Optional.empty`) and the `SetDataEvent` never
  published; `updateExpiration` and `restore` stored null with **no exception at all**
  — the snapshot-adopt path (`PartitionMap` → `KeyMap` → `KeyValue`) carrying the same
  silent-absence shape under the very guards 6(v) placed upstream of it.

The fix is the same two families 6(v) anchored, applied one layer down: the
**constructor-carrier** family — `DataVersion` now holds a true detached copy
(`Map.copyOf`), the null map refused as a *named* NPE (`"data must not be null"`, the
`Snapshot` "entries" / `LogsCommitted` "logs" precedent) and the null elements refused
with the copy's own NPE (the `Map.copyOf` element axis — type-only, the
`LogsCommitted` element-pin style, since that NPE carries no message); and the
**method-intake** family — all three `KeyValue` writes refuse a null version at the
method head in the IAE family (`"data must not be null"`) **before** anything is
stored, before an event is built, and before it is published.

**Pin (RED-confirmed before the fix, then GREEN), new `DataVersionTest` (5) and
`KeyValueTest` (3) in-package:** `DataVersionTest` — the null map is refused as NPE
**naming the axis** (`"data must not be null"`, 2-arg and 3-arg alike; was the JDK's
"'m' is null"); the **detached-copy pin** the javadoc already promised (a producer
rewrite or clear after construction leaves the stored value byte-identical; pre-fix the
rewrite was visible through the view); the null-key and null-value maps both refused at
the constructor (pre-fix: silently admitted); and the stored map itself rejects
mutation. `KeyValueTest` — each of the three writes refuses a null version IAE with the
axis named, with the **already-stored value surviving** the refusal (pre-fix
`setData`: key present but `getData()` empty and the event lost; pre-fix the other two:
no exception at all), and the legitimate value still readable at its version — the
intake usable after refusing.

Isolated: `DataVersionTest` **5 green** and `KeyValueTest` **3 green** —
**8 green / 0 fail / 0 skip**.
Full suite at this point: **397 tests green, 0 failures, 1 pre-existing skip**
(+8 over the 6(v) baseline of 389). The pointer+carrier family the house has been
closing since 6(g) now closes the last raw layer underneath it: the carrier every
stored value and every snapshot entry holds, and the write that stores it — refusing
the shapes its own sealed constructors can never produce, before a value is stored,
before a version is consumed, and before an event is published.

**Non-happy-path hardening pass, twenty-second round (2026-11-24, stage 6(x))** — the
partition-intake layer: 6(v) sealed the apply/data plane *at the boundary* — every
`PartitionMap` intake now refuses its null axes before a version is consumed or a
partition is touched — and 6(w) sealed the *carriers* those intakes store — but the
layer between them, `KeyMap`, had no gate of its own: it is the per-partition key
store **every** sealed `PartitionMap` intake delegates to, and the store
`StateMachineBridge` applies committed state into through. A construction-bypassed
caller — the 6(n)/6(p) shape at every intake above — or the apply path reaches it
directly:

- **null key into the four `KeyMap` intakes** (`setEntry` / `getEntry` / `deleteEntry`
  / `expireEntry`): `HashMap` admits null keys (construction-legit — `capture()` would
  never emit one, but a raw caller can), so `setEntry(null, dv)` installed a `KeyValue`
  under a null key **and published** the `SetDataEvent` naming an unnameable key — a
  silent install *with* a bus trace, the campaign's worst admission shape, precisely
  because the trace exists and could be read as legitimate; the read/delete/expire
  intakes served `Optional.empty` / `false` with **no exception at all** — asymmetric
  with the exact same axis one layer above (`PartitionMap.getData(null)` IAEs `"key
  must not be null"` since 6(v)).
- **null version into `setEntry` (the 6(w) shape, one layer up)**: the `KeyValue` shell
  was computed into the map FIRST, and only then did the deep IAE (`"data must not be
  null"`, sealed 6(w)) fire — the pin observed on the *map*, not the exception: a later
  `deleteEntry` **returned true** and published a `DeleteDataEvent` for a key that was
  never written.
- **null map / forged state elements into `restore`**: `restore(null)` NPE'd mid-
  iteration (the raw JDK shape where the `restore` one layer above — sealed 6(v) — IAEs
  naming the state); a construction-legit `HashMap` carrying a null key **cleared the
  partition first** (dropping the legitimate entries) and then installed the null-key
  phantom *successfully, with no exception at all*; a forged null value threw the
  version's `DataVersion` IAE **after** the clear (naming the version rather than the
  state slot) — mutate-then-throw with a partial install the `capture()` pin could
  reach. `PartitionMap.restore`'s whole-state pre-validation (zero state change) is
  the template `KeyMap.restore` lacked underneath it.
- **`PartitionMap` constructor carriers + `getPartitionForKey(null)`**:
  `Objects.requireNonNull(eventBus)` / `(clock)` carried **no message** — the one
  constructor in the data plane missing the house named-NPE rule (`Snapshot` "entries",
  `DataVersion` "data", `LogsCommitted` "logs") — and `getPartitionForKey(null)` NPE'd
  at `key.hashCode()` where every data-plane intake IAEs naming the axis.

The fix is the same two families 6(v) anchored, applied one layer down: **method
intake** — `"key must not be null"` at the head of all four `KeyMap` intakes, `"data
must not be null"` in `setEntry` **before** the shell is computed or any event built or
published, and `restore` pre-validating the WHOLE state map (a null state IAEs `"state
must not be null"`; every entry's key and value is refused naming the axis) **before**
the synchronized clear-install, so a refusal leaves the partition byte-identical — no
clear, no partial install, no phantom — mirroring the `PartitionMap.restore` template;
**constructor carriers** — the two `PartitionMap` NPEs now name the dependency
(`"eventBus must not be null"`, `"clock must not be null"`), and `getPartitionForKey`
refuses a null key in the IAE family.

**Pin (RED-confirmed before the fix, then GREEN), new `KeyMapTest` (8) in-package
(`lib.data`) and `PartitionMapTest` +2:** `KeyMapTest` — each of the four intakes
refuses a null key IAE naming the axis, with the zero-state-change discriminators
(`setEntry`: no phantom in `capture()` AND no event on the bus; `deleteEntry`: no
event; and `setEntry(key, null)`: a later `deleteEntry` still returns **false** — the
shell is not left behind, the observable pin the exception alone could not give);
`restore`'s null map / null-key / null-value refusals all IAE naming the axis, with the
installed state intact — one entry, its original value, nothing installed; `new KeyMap`
plus a live `PartitionMap` parent, so the bus pins observe the real publish path.
`PartitionMapTest` — the constructor refuses a null bus / null clock **naming the bus
and the clock** (pre-fix: the JDK's parameter-less NPE, message `null`), and
`getPartitionForKey(null)` IAEs `"key must not be null"` (pre-fix: raw NPE in
`hashCode`).

Isolated: `KeyMapTest` **8 green** + `PartitionMapTest` **16 green** —
**24 green / 0 fail / 0 skip**.
Full suite at this point: **407 tests green, 0 failures, 1 pre-existing skip**
(+10 over the 6(w) baseline of 397). The pointer+carrier family the house has been
closing since 6(g) now closes the layer its boundary sealed in 6(v) delegated to: the
key store every data intake writes through and every committed state applies through —
refusing the shapes the sealed intakes above it can never produce, before a shell is
installed, before a version is consumed, and before an event is published.

**Non-happy-path hardening pass, twenty-third round (2026-11-24, stage 6(y))** — the
fan-out root itself: 6(v) sealed the data plane at the boundary (`PartitionMap`
intakes), 6(w) its carriers, 6(x) the key store they write through — while the
`EventBus`, the shared bus **every** sealed subscriber subscribes onto
(`StateMachineBridge`, the leader's `AppendLog` subscriber, `Replica`) and every
publisher delivers through, had no gate of its own. Production wiring only ever
subscribes sealed subscribers and publishes sealed events, so the raw axes were
reachable by a construction-bypassed caller — the 6(n)/6(p) shape at every sealed
intake above — but at the root, that bypass IS the surface:

- **null subscriber into `subscribe`**: `CopyOnWriteArrayList` admits null elements
  (unlike the `Map.copyOf`/`List.copyOf` family 6(w) sealed on its carriers), so
  `subscribe(null)` parked a null entry **silently** — no exception, no trace — and
  the next legitimate `publish` NPE'd AT DISPATCH (`s.onEvent` on the null entry): a
  bus-level crash, with list order deciding the blast radius — subscribers before the
  parked null were served, the ones after it were STARVED of that event. A
  position-dependent failure with no exception naming the axis.
- **null subscriber into `unsubscribe`**: a silent no-op indistinguishable from
  removing a handle that was never subscribed — no axis named, no error surfaced.
- **null event into `publish`**: the null event fanned OUT to the subscribers — the
  production-shaped `instanceof`-filtering ones (`Replica`, the leader's `AppendLog`
  subscriber) silently swallow anything non-matching, **including null** — until it
  reached the sealed `StateMachineBridge` intake, whose 6(v) refusal (`"event must not
  be null"`) threw from DEEP inside the bus's fan-out loop, not from the bus's own
  intake: readable as a subscriber failure, and every subscriber after the bridge in
  list order never saw the event. 6(v) sealed the axis at the consumer side; the
  producer side still handed nulls across.

The fix is the house IAE family applied at the root, carrying the same zero-state-
change and zero-side-effect guarantees 6(v)/6(x) anchored in the data plane:

- `subscribe` / `unsubscribe` — `"subscriber must not be null"` at the head, BEFORE
  the list is touched: a null entry can never park, so dispatch can never crash on
  one and list-order starvation dies with it;
- `publish` — `"event must not be null"` at the head, BEFORE any subscriber is
  invoked: the same axis name the sealed `StateMachineBridge` intake already speaks
  on the consumer side, now spoken by the producer where the null originates; a
  refused publish fans out to exactly zero subscribers and leaves the bus
  byte-identical.

**Pin (RED-confirmed before the fix, then GREEN), new `EventBusTest` (4) in-package
(`lib.event`):** `subscribe(null)` IAEs naming the subscriber — the discriminator
observes the bus, not the exception: after the refusal, the recorder subscribed
before AND after the refused null are each served exactly one legitimate event
(pre-fix: the parked null NPE'd that same publish at dispatch, starving the later
subscribers in list order); `unsubscribe(null)` IAEs naming the subscriber and leaves
the installed subscriber still subscribed and still served; `publish(null)` IAEs
naming the event with the zero-fan-out discriminator — two null-tolerant recorders
(mirroring the production `instanceof` shape that swallows non-matches) each received
**nothing** (pre-fix: the null event rode to ALL subscribers silently, and was refused
only if a sealed bridge happened to sit in list order — in which case the exception
arrived mid-iteration and delivery past it aborted); `testLegitimateIntakeStillWorks`
pins subscribe → publish → served exactly once, unsubscribe → publish → not served
again, the intake shape every round's wiring above depends on.

Isolated: `EventBusTest` **4 green / 0 fail / 0 skip**.
Full suite at this point: **411 tests green, 0 failures, 1 pre-existing skip**
(+4 over the 6(x) baseline of 407). The fan-out family the house has been closing
since 6(g) now closes its root: the bus every production subscriber sits on and every
publisher delivers through — refusing a null subscriber before it parks, a null event
before any fan-out, speaking the same axis names its own sealed consumers speak.
