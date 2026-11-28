# Dynamic Cluster Membership (join / leave) — Investigation and Staged Plan

> **Status: DECIDED (Stage-0 gate passed, 2026-11-26) — OQ-1 = (b), OQ-2 = (a), OQ-4 = extend,
> OQ-6 = (i); OQ-3 and OQ-5 resolve derivatively from those; OQ-7 stays OPEN at its Stage-5
> gate** (each choice recorded in §4 with its trade-offs). Investigation + staged plan; no code
> yet — Stage 0's `SCOPE.md` flip is the first tracked diff (§5); the approved Stage-1 plan and
> the new-session startup are in §8. This is an untracked working
> handoff (like the client-track handoff it replaces): it is **never staged, never committed,
> never `git push`ed**. It advances work-plan item 4, which `SCOPE.md:472-476` records verbatim
> as:
>
> > 4. **Fixed cluster topology** *(user-deferred)* — membership is fixed at startup; changing it
> >    today means taking the cluster down and rolling it over. No join/leave protocol, no joint
> >    consensus, no member transitions. (`disconnectFrom`, Decision #3, has since been implemented
> >    as a link-level operation — it tears down individual live links, which composes with a fixed
> >    topology — but consensus-driven join/leave remains deferred under this entry.)
>
> Baseline: branch `finish`, HEAD `23e8f20` (client track, unpushed), 487 tests green, 1
> pre-existing skip (2026-11-26). House style: every code claim carries a file:line pin; open
> questions are flagged, not silently chosen (the `design/REMOTE-CLIENT.md` convention this and
> the `REMOTE-CLIENT-C` docs follow); the plan is staged with a discriminating pin per stage, the
> `SNAPSHOTS.md` §4 format this document copies.

---

## 1. Ground truth — what exists in the code today

### 1.1 Topology is per-node, in-memory, and self-seeded

- Each node keeps **its own** `ClusterTopology` and registers itself in the constructor:
  `RaftNodeImpl.java:103-105` — comment "Each node has its own view of cluster topology and self
  registers with it", then `this.clusterTopology = new ClusterTopology();` and
  `this.clusterTopology.register(new NodeIdentifier(id, nodeAddress));`.
- `ClusterTopology` is a bare `HashMap<String, NodeIdentifier>` with `register`/`unregister`/
  `getTopology` and `getMajorityCount()` = `size/2+1` — `ClusterTopology.java:6,18-32,34-36`.
- **Nothing in the log is applied to the topology.** No code path folds a log entry into the map;
  the map is filled exclusively by (a) the self-register above and (b) the connect-time handshake
  in §1.2. Membership is therefore *volatile, per-node, and unreplicated*.
- `ClusterTopology.unregister` **has zero call sites** anywhere in `src/main` or `src/test`
  — dead code in both directions today — `ClusterTopology.java:26-28`.
- `InMemoryTopologyDiscovery.removeKnownNode` is likewise unused (explore sweep, this pass).

### 1.2 The only "membership distribution" is the ad-hoc connect-time handshake

- Every outbound connection sends an `Introduction` (self id + address + current term/index):
  `RaftNodeImpl.java:231-236` (`connection.sendIntroduction(new Introduction(id, nodeAddress, …))`).
- The receiver **registers the introducer and replies with its full current topology**:
  `RaftNodeBehavior.onIntroduction`, `RaftNodeBehavior.java:41-74` — it refuses a malformed
  intro before binding (Stage 6(k) guard, `RaftNodeBehavior.java:41-52`), then admits the peer.
- `RaftNodeBehavior.onAnnounceClusterTopology`, `RaftNodeBehavior.java:77-126` — admits the
  announced list as one all-or-nothing transaction (refusing a null id or missing address,
  `RaftNodeBehavior.java:78-89` comment block) and binds the sender.
- `connectWithTopology()` → `newTopologyConnections()` walks the *local* topology and dials every
  entry: `RaftNodeImpl.java:411-413` (API), `RaftNodeImpl.java:196-215` (implementation, skipping
  self and already-connected peers).
- Consequence: a node's membership view is the **union of whatever each peer chose to announce, at
  the moment that link was opened**. There is no ordering guarantee, no shared source of truth,
  and no record anywhere of a membership decision having been *made* — it is a side effect of
  link establishment, not a replicated state.

### 1.3 Quorum is computed from the local volatile map, at decision time, everywhere

- Candidate, self-majority shortcut (1-node election): self-vote at `RaftNodeBehaviorCandidate.java:40`,
  gate `votes.size() >= self.clusterTopology.getMajorityCount()` at `:42`
  (shortcut block `:42-56`, `convertToLeader` at `:54`).
- Candidate, vote tally → leader conversion: `RaftNodeBehaviorCandidate.java:152-161`, majority
  gate at `:159`, `convertToLeader` at `:160`. Votes are a `Set<String>` keyed by node id
  (`RaftNodeBehaviorCandidate.java:17`, add at `:154`).
- Follower, Pre-Vote self-majority shortcut: `1 >= self.clusterTopology.getMajorityCount()` at
  `RaftNodeBehaviorFollower.java:107` (shortcut block `:105-112`); Pre-Vote grant tally → candidate:
  grant added at `:303`, majority gate at `:307`.
- Leader, commit quorum: `updateCommittedIndex()` — `RaftNodeBehaviorLeader.java:346-370`. It
  collects the **live active connections'** term-indices (`:347-353`), adds the leader's own log
  (`:360`, rationale comment `:355-359`), then gates on
  `self.clusterTopology.getMajorityCount()` (`:363-364`).
- Every one of those reads `clusterTopology` — the *local* map — at decision time. Two nodes that
  disagree about membership compute different majorities in the same term, and nothing in the
  protocol makes them agree.

### 1.4 Progress tracking is per-connection, not per-member

- The leader keeps log progress **on the connection object itself** (term-index per
  `NodeConnectionOutbound`); the only id-keyed structure in the leader is `snapshotsInFlight` —
  `RaftNodeBehaviorLeader.java:74,226,651`.
- An unknown node id has no progress record by construction; the commit-quorum count (§1.3) is
  driven by live connections, not by membership. Elections fan out to **all** active connections,
  not a membership list — `RaftNodeBehaviorCandidate.java:59-61`.
- Consequence for join: a new member that has not connected has nothing to replicate *to*; a
  removed member whose link is still live is still counted *in* — until someone severs the link,
  and nothing makes the quorum count check membership at all.

### 1.5 Disconnect is link-level only (and it is pinned)

- `disconnectFromInternal` removes the connection and closes the channel — no topology change,
  idempotent — `RaftNodeImpl.java:250-262`.
- Pins that must stay green: `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable` (2-node
  quorum: a 2-node leader with its only peer disconnected holds 1-of-2 and cannot commit) and
  `RaftNodeDisconnectTest` (3-node loopback cluster, 2-of-3 commits with a frozen peer,
  re-joining via `connectWithTopology()`). Recorded at `SCOPE.md:180-183,269-274`.

### 1.6 The write path and leader discovery (where membership meets clients)

- `submit()` refuses non-leaders outright — `RaftNodeImpl.java:491-500` (non-leader guard `:494-496`).
- Non-leader cache verbs reply `NOT_LEADER` with a leader hint (Decision: Redirect, variant 2 —
  `commit 23e8f20`, `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`):
  `RaftNodeBehavior.java:271-277,291-313`.
- `StateResponse.cluster` carries the node's **local** topology view for client re-discovery:
  `RaftNodeBehavior.java:215-230`, serialized at `WireCodec.java:345-346` (additive proto field 7,
  B §4.2). A client that re-discovers after a membership change sees whichever node it asked —
  another instance of §1.1's per-node map leaking to the outside.

### 1.7 The wire surface is frozen and closed — and its failure modes are loud

- `WireMessage` oneof: 20 variants, 1–14 raft/handshake, 15–20 cache client verbs —
  `wire.proto:324-350`. **Append-only** per the compatibility rules; the next new frame is 21+.
- `RaftLog.entry` oneof: 2–5, ending `string_entry = 5` — `wire.proto:110-118`. The header
  comment for 5 matters: it advances the applied index "with no state-machine action — modeled
  honestly as an explicit string variant rather than a typed hole" (`wire.proto:108-109`). The
  next new log-entry carrier is 6+, same file, same frozen rule.
- The codec is the single boundary and it is explicitly closed: `WireCodec.FRAME_VARIANTS = 20` —
  `WireCodec.java:74`; `serialize` rejects any POJO outside the closed set with a named error
  (`WireCodec.java:155-158`); `deserialize` switches on `message.getMessageCase()` and **throws**
  `InvalidProtocolBufferException` on `MESSAGE_NOT_SET` / unknown (`WireCodec.java:170-218`, the
  throw at `:213-217`).
- **Interop consequence (verified this pass):** a receiver whose generated proto knows only
  variants 1–20 decodes a 21+ frame as `MESSAGE_NOT_SET` — i.e. a *newer* node's membership frame
  is a **loud, throwing refusal on an older node**, and the reverse never occurs because an older
  node cannot send those variants at all. Mixed-version behavior is one-directional by
  construction; whether that is the boundary we want is OQ-6.
- Same closed-set discipline inside the log: `WireCodec` maps `AppendEntries.RaftLog` against the
  four applied entry types and **throws** on any other (`WireCodec.java:291-311`, throw
  `:304-308`) — a new log-entry carrier must extend this dispatch explicitly.
- `WireSerializationTest` pins the variant-number table (1–20) at `:403-430` (expected map
  `:405-426`, closed-set length pin `:404`, `MESSAGE_NOT_SET` skip `:427-429`) and round-trips
  each frame through the codec at `:435-475`; every new variant needs its row in both.

### 1.8 Dead stubs pointing at exactly this feature

- `ClusterJoinRequest` — a 14-line POJO, `nodeId` only, **zero references in the codebase**:
  `proto/raft/communication/ClusterJoinRequest.java:1-14`. It is the only trace of a join verb's
  intended shape.
- `ClusterTopology.unregister` (zero call sites, §1.1), `InMemoryTopologyDiscovery.removeKnownNode`
  (unused, §1.1).

### 1.9 The invariants already pinned by tests (the green line this work must not cross)

`RaftNodeBehaviorFollowerInstallSnapshotTest` (announce admission: valid / unresolvable /
null-reject, ≈`:565+`, `:644`, `:673`, `:696`); `WireSerializationTest` (`:118-123`, `:439`,
`:403-472`); `RaftNodeDisconnectTest`; `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable`;
`RaftNodeBehaviorPreVoteTest`. Baseline at delivery: **487 green, 1 pre-existing skip** (2026-11-26).

---

## 2. Gap analysis — why "add a join message" is not safe yet

1. **Divergent configurations ⇒ two correct leaders.** Membership lives in per-node volatile maps
   (§1.1), and quorum reads those maps at decision time (§1.3). If node A's map says
   {A,B,C} (majority 2) and B's says {A,B} (majority 2), a partition puts A+C committing at 2-of-3
   *and* B believing it cannot commit at all — or, worse, both sub-clusters committing with
   different intersection semantics. The Raft paper addresses precisely this: reconfiguration
   requires joint configurations so that any quorum of the old config intersects any quorum of
   the new one; `raft-notes.md:108-124` quotes the §6 passage on removed servers starting
   disruptive elections (the paper's own statement of the hazard), and notes the paper contains
   no Pre-Vote (§9.6 territory, `raft-notes.md:210`).
2. **There is no shared object of decision.** The only replicated memory is the log, and the log
   has no membership carrier (§1.7: `RaftLog.entry` ends at 5). A "join" today can only be
   *announced*, never *decided* — announcement is a local side effect of a handler
   (`RaftNodeBehavior.java:77-126`), not a log entry all replicas apply in the same order.
3. **Quorum consumers have no notion of "membership change in flight".** The three quorum sites
   (§1.3) all read one map; there is no concept of a pre-change vs post-change config, no
   "applied" vs "proposed" membership, no joint window. Any change to how quorum is computed
   touches candidate (`:42`, `:159`), follower (`:107`, `:307`) and leader (`:363-364`) at once —
   that coupling is the reason this work must be staged with a discriminating pin at each of them.
4. **No one can be removed.** Leave does not exist: `unregister` is dead (§1.1, §1.8), and the
   only "leave" that exists is `disconnectFrom` — link-level, local, one-directional
   (`RaftNodeImpl.java:250-262`), pinned to be exactly that (§1.5). A disconnected node is not a
   departed member; it still counts toward no one's config and still votes as if it were in one
   the moment it reappears (the rejoin path in `RaftNodeDisconnectTest` is the evidence).
5. **Bootstrap is handshake, not protocol.** §1.2's `Introduction`/`Announce` flow admits whatever
   arrives at a fresh link; there is no authentication against a shared record (flagged as such by
   the Stage 6(k) comment — `RaftNodeBehavior.java:78-83`). Dynamic membership either makes this
   handshake the *delivery channel* for a shared decision (keeping it), or supersedes it with a
   log-derived truth (retiring it) — that is OQ-5, and it cannot be answered without OQ-2 first.

---

## 3. Invariants (what the delivery must keep true, with its current pins)

- **N1 — One leader per term, even under divergent views.** The whole point of §6/joint
  semantics; today it holds only because membership is in fact identical across a healthy cluster
  (same startup seed). Dynamic membership must not break it on the change path.
- **N2 — Every commit is witnessed by a quorum of a *single shared* configuration.** In joint
  form: quorum-of-C_old ∩ quorum-of-C_new ≠ ∅. In single-config-change form: a membership entry
  commits under the pre-change config and *no entry applies* (to quorum math) until committed.
  Neither exists today; this is the new invariant this work creates.
- **N3 — Wire is append-only.** `WireMessage` variants append from 21+; `RaftLog.entry` appends
  from 6+; numbers 1–20 / 2–5 are frozen (`wire.proto:110-118,324-350`); `WireCodec`'s closed set
  (constant `:74`, both dispatches, error messages) extends *as a unit*.
- **N4 — The existing pins stay green** (§1.9): the 2-node quorum pin, the disconnect/rejoin pair,
  Pre-Vote tests, and the announce-refusal tests — 487 is the floor, and each stage re-runs
  `mvn -o -B test` four times green before its commit.
- **N5 — The log is the only shared memory.** Any replicated state that affects quorum or the
  state machine is reached *through* a log entry applied on all members in log order. (Bounded by
  the current no-durability reality — persistence is itself a separate deferral
  — `SCOPE.md` item "Spill data out of memory to a consistent (durable) store", also
  user-deferred.)
- **N6 — Do-not-touch.** `RaftNodeImpl.java:164-168` (`TODO: MEGA WEIRD!!!`), the B/C design
  section numbering, `InstallSnapshotExample` (working notes, do-not-modify per `SCOPE.md` item 5).

---

## 4. Open questions — flagged with trade-offs; Stage-0 gate decided 2026-11-26

> House convention (`design/REMOTE-CLIENT.md`): the options are stated with their trade-offs and a
> recorded lean, and the user decides at the gate before any code moves. That gate has now passed
> (2026-11-26): **OQ-1 = (b), OQ-2 = (a), OQ-4 = extend, OQ-6 = (i)** — each recorded below with
> the user's choice. OQ-3 and OQ-5 resolve derivatively from those; **OQ-7 is explicitly deferred
> to the Stage-5 gate.**

### OQ-1 — Consensus mode: joint (paper §6) vs single-config-change entry
- **(a) Joint consensus** — cluster runs `C_old ∪ C_new` while a change is in flight; commits
  during the window need a quorum of `C_old ∩ C_new`. The paper's general answer (§6; the
  disruption passage is quoted at `raft-notes.md:108-124`). Most code: every quorum consumer in
  §1.3 gains an "am I in a joint window?" branch, and leader commit (`RaftNodeBehaviorLeader.java:363-364`)
  needs two-majority math.
- **(b) Single-config-change entry, one change at a time** — append a membership entry, commit it
  under the *old* config, then apply the new config atomically on all members (in log order). One
  config object per node stays true (the whole existing shape of §1.3 is left alone except that
  *what the map contains* becomes log-derived); no concurrent changes in flight.
- **Recorded lean (not a decision): (b)** — it matches the existing code shape (one
  `ClusterTopology` per node, quorum = `size/2+1`, `ClusterTopology.java:34-36`), and the
  paper's §6 machinery exists to generalize exactly this simpler case.
- **DECIDED (2026-11-26, Stage-0 gate): (b)** — single-config-change entries, one member at a
  time: the membership entry commits under the *pre-change* configuration and applies atomically
  in log order; the quorum sites keep their current single-map shape (N2 in single-config form);
  multi-member changes serialize. The §6 joint window (and its two-majority math) is out of scope
  by decision.

### OQ-2 — Carrier: replicate the change, or just message it?
- **(a) New `RaftLog.entry` variant (6+)** — e.g. a typed membership payload modeled on
  `string_entry = 5` (`wire.proto:110-118`), applied by the state machine on replay/snapshot
  restore. The codebase's own convention for non-data entries already exists (the "explicit string
  variant rather than a typed hole", `wire.proto:108-109`).
- **(b) Message-only** — a new `WireMessage` 21+ verb that extends the announce handshake
  (`RaftNodeBehavior.java:77-126`) without any log carrier. *Least work, and it leaves N2
  unsatisfied and membership unsurvivable of a restart (N5): flagged as the weak option, but
  recorded because it is the path of resistance.*
- **Recorded lean: (a).**
- **DECIDED (2026-11-26, Stage-0 gate): (a)** — a new `RaftLog.entry` variant 6+ is the replicated
  membership carrier (replicated, survives restart, satisfies N2/N5). This composes with the OQ-4
  decision, so Stage 1 adds *both* wire halves: the 21+ client verb and the 6+ log carrier.

### OQ-3 — Quorum during the change
Who counts, and when? Concretely: does the commit gate
(`RaftNodeBehaviorLeader.java:363-364`) and the three election gates (§1.3) count the pre-change
config, the post-change config, or live connections, *during* a membership change in flight?
Today it counts "live connections + self" — which is only correct if the config is shared
(§1.3), which is not (OQ-1 decides the answer for each mode; (a) = two-majority, (b) = old
config until the entry's commit index is applied).
**RESOLVED (derivative, after the 2026-11-26 decision):** with OQ-1 = (b), the answer is fixed —
every quorum gate counts the **pre-change configuration** until the membership entry has committed
*and* applied in log order; there is no in-flight window, so no gate grows a membership-state
branch.

### OQ-4 — The `ClusterJoinRequest` stub
Unreferenced 14-line POJO (`proto/raft/communication/ClusterJoinRequest.java:1-14`). Extend it to
be the client-facing join/leave verb surface, or delete it and design the API afresh alongside
the OQ-2 carrier. (It is the only trace of an intended join shape; deleting is a visible act that
should be user-sanctioned.)
**DECIDED (2026-11-26, Stage-0 gate): keep and extend** — `ClusterJoinRequest` remains the
client-facing join/leave verb surface (extended with the payload it lacks, e.g. the member's
address); it is not deleted, and no fresh API is designed around it.

### OQ-5 — Relationship to the `AnnounceClusterTopology` bootstrap
Does join *keep* the announce handshake as the transport for initial topology delivery (log decides
*what*, announce merely *delivers*), or does membership become fully log-derived and the announce
path is deprecated? This depends on OQ-2: log-derived truth (lean (a)) makes announce redundant
for members and still needed for non-member discovery. The Stage 6(k) refusal guards
(`RaftNodeBehavior.java:78-89`) must survive either way.
**RESOLVED (derivative from OQ-2 = (a); confirmed by the user with the Stage-1 plan at the 2026-11-26 handoff, recorded as the derived
consequence rather than an explicit choice):** membership truth is log-derived, so the `Announce`
handshake is kept as *delivery / initial transport* — the discovery channel for non-members — and
its admission will validate *against the log* (Stage 2). The Stage 6(k) refusal guards stay green
as the N4 floor either way.

### OQ-6 — Mixed-version interop (verified hazard)
A 21+ frame is a **throwing refusal** on an older binary (`WireCodec.java:213-217`) — one-directional
incompatibility (§1.7). Options: (i) accept the refusal as the boundary (membership verbs simply
fail across versions — honest, zero new protocol); (ii) capability negotiation in the existing
`Handshake`/`Acknowledgement` frames 13–14 (`WireCodec.java:125-130`); (iii) a version check before
a membership frame is ever sent.
**DECIDED (2026-11-26, Stage-0 gate): (i)** — accept the loud, throwing refusal as the
mixed-version boundary: membership frames simply fail across an old/new mix, honestly, with zero
new protocol. Stage 1's mixed-version test pins the refusal *as behavior* (asserting the
`WireCodec.java:213-217` throw on a 21+ frame), so the boundary is discriminated, not assumed.

### OQ-7 — Deposing a removed member (the paper's named disruption)
`raft-notes.md:110-118` quotes the exact failure: a removed server keeps running elections. The
current code has no leader-lease / "I heard from a leader recently" voter guard (Pre-Vote is the
implemented mitigant, `RaftNodeBehaviorFollower.java:100-108`). When a member is removed, how does
it learn it is gone — retain-and-deliver the removal entry then sever the link (the Stage-5
design), or disconnect-after-commit and trust Pre-Vote? The §6 passage the paper itself proposes
(voters ignore RequestVote within the minimum election timeout of a live leader) is *not*
implemented here — flag, don't assume it.
**OPEN (2026-11-26, Stage-0 gate): explicitly deferred to the Stage-5 gate**, where the user will
choose deliver-then-sever vs disconnect-after-commit; Stages 1–4 pre-commit neither answer.

---

## 5. Staged plan (SNAPSHOTS.md §4 format; each stage ships with its discriminating pin)

> Stage 0 (the gate) passed on 2026-11-26; with OQ-1 = (b), OQ-2 = (a), OQ-4 = extend and
> OQ-6 = (i) recorded — and OQ-3/OQ-5 following — the rows below are the *chosen* shape
> (OQ-7 remains open at its Stage-5 gate). Every stage: change, then the *discriminating* test —
> the test that fails if the stage is missing or wrong and passes if it is right (the house
> format's definition of a pin).

| Stage | Change | Discriminating pin |
|---|---|---|
| **0 — gate** ✅ decided 2026-11-26, **DELIVERED 2026-11-27** (SCOPE.md flip committed in 53e7bcb) | OQ-1 = (b), OQ-2 = (a), OQ-4 = extend, OQ-6 = (i) by the user; OQ-3/OQ-5 resolved derivatively; OQ-7 explicitly deferred to Stage 5 — all recorded in §4, header DECIDED above. `SCOPE.md:472-476` item 4 flips "user-deferred" → "in flight per MEMBERSHIP.md" (the first tracked commit of this track). No other code. | Decision text + DECIDED header exist in this file (the gate's own deliverable); then the `SCOPE.md` flip, under the 4×-green discipline. |
| **1 — wire** (both halves, per OQ-2 = (a) + OQ-4 = extend) — **DELIVERED 2026-11-27, commit 53e7bcb** (4× green; floor raised 487 → 489) | New `WireMessage` variant(s) 21+ (the `ClusterJoinRequest`-carried client join/leave verb) **and** a new `RaftLog.entry` variant 6+ (the replicated membership entry) in `wire.proto` (append-only); `WireCodec` closed set extended as a unit: `FRAME_VARIANTS` (`:74`), `serialize` (`:88-159`), `deserialize` (`:170-218`), the `RaftLog` dispatch (`:291-311`), both error messages. | `WireSerializationTest`: variant-number table `:403-430` gains one row per new variant (the expected map is `:405-426`, the closed-set length pin `:404` — today 20 — grows by the added count; the `MESSAGE_NOT_SET` skip is `:427-429`); one round-trip per new type at `:435-475`; **mixed-version discriminator**: bytes with oneof field 21 parsed through a 1–20-only definition decode to `MESSAGE_NOT_SET`, and `WireCodec.deserialize` throws `:213-217` — the test asserts the *refusal*, pinning the decided OQ-6 = (i) boundary as behavior. |
| **2 — log-derived topology** *(enabled by OQ-2 = (a), decided 2026-11-26)* — **DELIVERED 2026-11-27, commit 93e3a99** (4× green; floor raised 489 → 491) | `ClusterTopology` contents become applied state: a replay fold (log entries → map) + startup reconstruction that validates against the self-register (`RaftNodeImpl.java:103-105`); `Announce` may now validate *against the log* (OQ-5). | **Restart discriminator**: after a membership entry in the log, a node rebuilt from log replay has exactly the map the running node had — volatile map re-seeding fails this. N4 floor: `RaftNodeTest#testDisconnectOnlyPeerQuorumUnavailable` and the §1.9 set all green (487+). |
| **3 — leader-driven join** *(OQ-1 = (b) + OQ-3 resolved: commit under the pre-change config, no joint window)* | Leader appends the membership-add entry on the write path (`RaftNodeImpl.java:491-500`); commits under the **pre-change configuration** (single-map majority — no two-majority branch); on commit every member applies per N5; then links the newcomer via the existing `connectWithTopology`/`newTopologyConnections` machinery (`RaftNodeImpl.java:411-413,196-215`). | **2→3 loopback IT**: post-join, the majority pin reads 2-of-3 (leader + either peer commits; leader alone does not — the existing 2-node pin generalized). **Safety discriminator**: while a join is in flight (entry appended, not yet committed), a commit attempt witnessed only by the pre-change config *minus* one member must fail — two-leaders-in-same-term is the exact failure class this kills. |
| **4 — newcomer catch-up** | The new member fetches the log (suffix) **and** the snapshot via the existing stage-5 pipeline (`RaftNodeBehaviorLeader` `SnapshotTransfer`, `snapshotsInFlight` `:74,226,651`; follower install per `SNAPSHOTS.md` stage 4); applies the config *before* it is quorum-countable. | **Late-member discriminator**: the newcomer starts empty, ends at the committed term/index; until its apply catches up, a commit that would *need* its count must not reach a majority — i.e. a "register and immediately quorum-count" bug (the `getMajorityCount()` inflation the announce-refusal guards against, `RaftNodeBehavior.java:78-83`) is caught here. |
| **5 — leave** *(OQ-7 gate: user chooses deliver-then-sever vs disconnect-after-commit here, before this stage's commit)* | Leader appends the membership-remove entry; on commit members apply it (config shrinks, N2 holds); the removed member is then severed via the existing link-level `disconnectFrom` (`RaftNodeImpl.java:250-262`); `ClusterTopology.unregister` (`:26-28`) finally gets its first call sites. | **3→2 IT with a departed voter**: post-leave, the removed node — still running, still holding stale majority of its old view (the `raft-notes.md:110-118` scenario) — cannot win an election: its votes are outside the config (gate `RaftNodeBehaviorCandidate.java:159`) and its pre-vote is refused (`:307`). **Idempotency discriminator**: a second leave of the same member is a no-op, mirroring the disconnect idempotency pin (`SCOPE.md:269-274`). |
| **6 — election hardening + partition** | Quorum consumers keep reading the single current config — **no joint-window branches anywhere** (OQ-1 = (b) decided, so the `:42/:159`, `:107/:307`, `:363-364` gates keep their single-map math); add a leader-lease voter guard only if OQ-7's Stage-5 gate chooses one. | **Post-change partition discriminator**: split the cluster *immediately after* a join/leave commits; the two sub-clusters must not both advance commits in the same term — N2 under live conditions, discriminated by term/index divergence that Pre-Vote alone (`RaftNodeBehaviorPreVoteTest`) does not exercise across a membership boundary. |

**Exit criteria for the whole track:** Stage 6 green; baseline floor 487+ at every commit;
`WireSerializationTest` table current; OQ-7 decided at its Stage-5 gate before Stage 5 commits;
`SCOPE.md` item 4's "user-deferred" resolved (Stage 0, done 2026-11-26); and this file's header —
already DECIDED as of the Stage-0 record — reaches its *delivered* state (the `SNAPSHOTS.md`
header format, stage/commit column filled in).

---

## 6. Boundaries and discipline (for the implementer)

- **Java 17 floor.** Four consecutive green `mvn -o -B test` before any commit in this track;
  stage specific paths only; **never `git push`** (push is the user's act); this file itself is
  never staged or committed.
- **Wire is frozen where frozen**: 1–20 (`WireMessage`), 2–5 (`RaftLog.entry`) are immutable;
  new numbers append only (21+ / 6+), same file (`wire.proto:110-118,324-350`).
- **Do-not-touch**: `RaftNodeImpl.java:164-168` (`TODO: MEGA WEIRD!!!`); B/C design section
  numbering; `InstallSnapshotExample` (working notes, `SCOPE.md` item 5).
- **Spec discipline**: any future revision of this file keeps every code claim at file:line +
  behavior; new open questions join §4, they are never folded into a stage silently.
- **No unflagged scope creep**: fault tolerance beyond the pins, gossip, security/authentication
  of identity claims, and durability are all separate deferred items — none of them are smuggled
  into a stage here.

## 7. Provenance

- `SCOPE.md:472-476` — item 4, the deferral this document advances (quoted verbatim in the header).
- `raft-notes.md:108-124` — paper §6 reconfiguration passage, quoted in-repo; `:210` — thesis §9.6 /
  Pre-Vote framing; `:64` — etcd default-on note.
- `SNAPSHOTS.md` — house format and the stage-4/5 snapshot pipeline that Stage 4 reuses.
- `design/REMOTE-CLIENT.md` — open-question convention; `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`
  — the decision-record format ("Status: DECIDED …") this header now follows (Stage 0, decided 2026-11-26).
- Code: `RaftNodeImpl.java` (103-105, 196-215, 231-236, 250-262, 411-413, 491-500);
  `RaftNodeBehavior.java` (41-74, 77-126, 215-230, 271-277, 291-313); `RaftNodeBehaviorCandidate.java`
  (17, 40-56, 59-61, 152-161); `RaftNodeBehaviorFollower.java` (105-112, 301-307);
  `RaftNodeBehaviorLeader.java` (74, 226, 346-370, 651); `ClusterTopology.java` (6-36);
  `WireCodec.java` (74, 88-159, 125-131, 170-218, 291-311, 345-346); `wire.proto` (104-118, 324-350);
  `ClusterJoinRequest.java` (1-14, dead).
- Test sets: §1.9; baseline 487 green / 1 pre-existing skip, branch `finish` @ `23e8f20`
  (unpushed), 2026-11-26.

---

## §8 — Handoff to the next session (2026-11-26)

### 8.1 State at handoff

- Branch `finish`, HEAD `23e8f20`, **unpushed**; `git status --porcelain` shows only `?? MEMBERSHIP.md`.
- Baseline verified 2026-11-26 with `mvn -o -B test`: **487 green, 1 pre-existing skip**. This is the floor until Stage 1's new tests land, after which the floor is `487 + added`.
- Stage 0 (the gate) is complete: §4 carries every decision with its trade-offs and dated record; the header says DECIDED. Stage 1's shape is **approved**; Stages 2–6 stand as planned in §5; OQ-7 is the only question still open and is gated at Stage 5.

### 8.2 Discipline — read before touching anything

- **Never `git push`** — that is the user's act, every time, without exception.
- **`MEMBERSHIP.md` is never staged and never committed** — a working handoff, not code.
- A tracked commit only after **4× consecutive green `mvn -o -B test`** (offline mode `-o`, batch `-B`); the baseline floor rises by every test added.
- **Stage specific file paths only** — never `git add -A`; `git status` must show nothing unexpected staged.
- **Do not touch** the `TODO: MEGA WEIRD!!!` block at `RaftNodeImpl.java:164-168`.
- `wire.proto` numbers are FROZEN — append-only: `WireMessage` oneof gets only 21+, `RaftLog.entry` oneof gets only 6+. Never renumber, never reorder.
- Do not renumber the B design-section numbering; do not modify `InstallSnapshotExample`.
- Java 17 floor.
- House spec discipline: every code claim carries a file:line pin; open questions are flagged with trade-offs, never silently chosen.
- **Generated protobuf sources are NOT tracked**: `protobuf-maven-plugin` (`pom.xml:85-88`, protoc 4.35.0) regenerates them into `target/generated-sources/protobuf` at every build. Commits contain only `wire.proto`, hand-written `.java`, and tests.
- `ClusterJoinRequest` is a **hand-written POJO** (not protobuf-generated): `nodeId` only, `ClusterJoinRequest.java:1-14`, currently dead code.

### 8.3 Approved Stage-1 plan (wire — both halves, as one unit)

1. **`wire.proto`** (append-only, both oneofs):
   - `WireMessage` oneof (`:324-350`): new variant **21+** — the client join/leave verb. Its payload extends the `ClusterJoinRequest` POJO with what it lacks (the member being added — e.g. its address — plus the join/leave operation itself); the POJO gains fields, nothing is deleted (OQ-4 = keep and extend).
   - `RaftLog.entry` oneof (`:110-118`): new variant **6+** — the replicated membership entry (OQ-2 = (a)); the `string_entry = 5` precedent and its rationale comment (`:104-109`) are the house model for a non-data log-entry carrier.
2. **`WireCodec`** closed set, extended as a unit (`src/main/java/com/github/jocull/raftcache/lib/raft/messages/WireCodec.java`): `FRAME_VARIANTS` (`:74`); `serialize` (`:88-159`, error at `:155-158`); `deserialize` (`:170-218`, throw at `:213-217`); the `RaftLog` dispatch (`:291-311`, else-throw at `:304-308`); both error messages.
3. **`WireSerializationTest`** (`src/test/java/com/github/jocull/raftcache/lib/WireSerializationTest.java`):
   - Variant-number table `:403-430`: one row per new variant in the expected map (`:405-426`); the closed-set **length pin `:404`** (`assertEquals(20, …)`) grows by the added count (20 → 20+added); the `MESSAGE_NOT_SET` skip stays (`:427-429`).
   - Round-trips `:435-475`: one per new frame type; the envelope tag bound asserted at `:472-474` widens by the added count.
   - **Mixed-version discriminator — the OQ-6 = (i) pin**: bytes of a 21+ frame parsed through a 1–20-only definition (`WireMessage.parseFrom` on the serialized bytes) decode to `MESSAGE_NOT_SET`; then a direct `WireCodec.deserialize` on those bytes **throws** the `WireCodec.java:213-217` refusal. The test asserts the *refusal* — the mixed-version boundary is a pinned behavior, not an assumption.
4. **Commit (first tracked commit of this track)**, only after 4× green `mvn -o -B test` with the new tests included:
   - `src/main/proto/wire.proto`; the new `ClusterJoinRequest` fields; `WireCodec.java`; `WireSerializationTest.java`;
   - plus the Stage-0 flip in `SCOPE.md:472-476` — item 4's `(user-deferred)` → in flight per `MEMBERSHIP.md` (word it precisely: consensus-driven join/leave is now in flight under this plan);
   - `MEMBERSHIP.md` stays out of the commit; verify `git status` before and after.

### 8.4 What comes after Stage 1 (pointers; §5 has the full rows)

- **Stage 2 — log-derived topology**: fold the per-node map into log-derived truth (`ClusterTopology.java:6-36`, dead `unregister :26-28`, majority at `:34-36`); `Announce` demoted to delivery/initial-transport, admission validates against the log (OQ-5, confirmed 2026-11-26); Stage 6(k) refusal guards (`RaftNodeBehavior.java:78-89`) stay green.
- **Stage 3 — leader-driven join**: append on the write path (`RaftNodeImpl.java:491-500`); commit under the **pre-change config** — single-map majority, no joint window (OQ-1 = (b)); link the newcomer via `connectWithTopology` / `newTopologyConnections` (`RaftNodeImpl.java:411-413,196-215`).
- **Stage 4 — snapshot pipeline for the joiner**: reuse the `SNAPSHOTS.md` stage-5 pipeline; `InstallSnapshotExample` untouched.
- **Stage 5 — leave**: **OQ-7 gate first** — user chooses deliver-then-sever vs disconnect-after-commit *before* this stage commits; removed member severed via `disconnectFrom` (`RaftNodeImpl.java:250-262`); `ClusterTopology.unregister` (`:26-28`) gets its first call sites; the `raft-notes.md:110-118` removed-voter scenario is the discriminating case.
- **Stage 6 — election hardening + partition**: quorum consumers keep their single-map math (`RaftNodeBehaviorCandidate.java:42/:159`, `RaftNodeBehaviorFollower.java:107/:307`, `RaftNodeBehaviorLeader.java:363-364`); leader-lease voter guard only if OQ-7's gate picks one.
- **Track exit criteria**: §5's closing paragraph, verbatim.

### 8.5 Immediate next move

Stage 1 executed and committed 2026-11-27 (commit 53e7bcb; 4× consecutive green `mvn -o -B test`, 489 green / 1 pre-existing skip). Stage 2 executed and committed 2026-11-27 (commit 93e3a99; 4× consecutive green, 491 green / 1 pre-existing skip — **floor is now 491**). Stage-0, Stage-1 and Stage-2 rows in §5 are ticked DELIVERED. Next: **Stage 3 — leader-driven join** per §5's Stage-3 row and §8.4: leader appends the membership-add entry on the write path (`RaftNodeImpl.java:491-500`), commits under the **pre-change configuration** (single-map majority, no joint window — OQ-1 = (b)), members apply per N5 (Stage-2's fold already does this), then links the newcomer via `connectWithTopology` / `newTopologyConnections` (`RaftNodeImpl.java:411-413,196-215`); pin = the 2→3 loopback IT with the in-flight-join safety discriminator. All §8.2 discipline still applies (4× green, stage the specific paths only, never `git push`, `MEMBERSHIP.md` never tracked).

