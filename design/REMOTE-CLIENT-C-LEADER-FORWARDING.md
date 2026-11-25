# Option C — Leader forwarding (any surviving node accepts a write, non-leaders forward)

> **Status:** design/spike only — not implemented. Under-specified where implementation will
> discover detail; concrete where the code already fixes the answer. Parent:
> `design/REMOTE-CLIENT.md`.
>
> **One-line:** turn "a write must be sent *to the leader's JVM*" into "a write can be sent
> *to any node* — a non-leader forwards it to the current leader and returns the leader's commit
> result." This is the small, self-contained enabler that both A and B benefit from.

---

## 1. High-level view (for a human reviewer)

```
                 client (in-process facade today; a remote client later via B)
                          │  put / delete / expire   — may land on ANY node
                          ▼
        ┌────────────────── any node ───────────────────┐
        │                                                │
        │  I'm the LEADER?  ─yes─>  submit(entry)        │   ← the existing path;
        │                            (commit → apply →   │     apply-then-ack preserved.
        │                             acknowledge)        │     Returns committed RaftLog<op>.
        │                            ──> reply(result)    │
        │                                                │
        │  I'm NOT the leader?  ─→  getLeader()         │   ← discover current leader
        │                       → forward the SAME entry│     (new node↔node message)
        │                         to the leader         │
        │                       ← relay the leader's    │
        │                         commit result back    │
        └────────────────────────────────────────────────┘
```

- **The single new capability:** if a write arrives at a node that is *not* the leader, that
  node forwards the exact same write entry to the leader and hands the leader's commit result
  back to the caller — instead of refusing.
- **Every other node in a quorum is now a valid write entry point.** This removes the
  "co-located with the leader" constraint without touching Raft semantics: the follower does
  *not* commit, apply, or hold a divergent copy — it hands the entry up to the leader, which
  commits it exactly once under the leader, and the follower picks it up through normal
  `AppendEntries`/`InstallSnapshot` replication like any other entry.
- **It stays entirely in the node↔node world.** The forward is a regular node-to-node message
  between already-introduced peers. It does **not** need B's foundational "how do we tell a
  *client* connection from a *Raft-peer* connection?" decision (Option B §4.1). That is the main
  reason C is small and low-risk: it reuses the well-understood pre-introduction/post-introduction
  dispatch that already works for `AppendEntries`, `InstallSnapshot`, and `StateRequest`.

**Why it's attractive:** it is the smallest, most self-contained increment of the three. It
changes only the write path's *accept* rule (leader-only → any-node), it reuses `submit`,
`getLeader`, and the existing request/response correlation verbatim, and it composes with both
the in-process facade (A's territory) and the remote client (B's). Its principal limit: it is a
**write** forwarding story; reads can stay local (eventually consistent on a follower) — that is
deliberately out of C's scope.

---

## 2. What already exists that C reuses (no changes)

- **The exact write entries** — `KeyDataInput` (put), `KeyDataDeleteInput` (delete),
  `KeyDataExpireInput` (expire); all `java.io.Serializable` and the *same objects* the leader
  appends today. A forwarded write is literally the object the in-JVM facade already
  `submit`s via `DistributedCache` (`leader.submit(new KeyDataInput(key, data))`, etc.).
- **Leader commit + apply-then-ack** — `RaftNode#submit` →
  `RaftNodeBehaviorLeader#updateCommittedIndex` (majority) → `RaftLogs#commit` (returns logs,
  does **not** ack) → `LogsCommitted` published → `StateMachineBridge` applies to the `PartitionMap`
  (synchronous) → **only then** `RaftLogs#notifyCommitted` completes the `CompletableFuture<RaftLog<…>>`.
  The leader-side handler in C calls the *same* `submit`; it does **not** re-implement any of
  this. The client-visible result of a forwarded write is the leader's `RaftLog<op>` — the
  committed term/index.
- **Leader discovery (in-JVM, already present)** — `RaftNode#getLeader()` →
  `getClusterNodeStates()` → `StateRequest` to active peers → pick `LEADER` at max term. The
  forwarder uses this to learn *where* to send.
- **Request/response correlation (generic — reuse verbatim)** — `Request.requestId` (UUID),
  `Response(Request)` echo, `NodeRequests#getRequestFuture(req, respClass)` +
  `completeRequest(resp)` (a pending-future map keyed by `requestId`; currently a **5 s** timeout
  — in-code TODO "Is this timeout appropriate?"). C's forward request reuses this exactly the
  way `sendStateRequest`/`StateResponse` does.
- **Transport + dispatch** — `ChannelMiddleware`/`ChannelSide`; `SocketChannelMiddleware` for
  real sockets; `NodeConnectionOutboundImpl#receive(Object)` is the `instanceof` dispatch chain
  (two-phase: pre-introduction vs post-introduction). The forward message is a **post-introduction**
  message (it travels between already-introduced peers), so it slots into the same phase as
  `AppendEntries`/`InstallSnapshot`.

---

## 3. New pieces (proposed; names tentative)

### 3.1 A node↔node **forward request + response** pair (in `lib/raft/messages/`)

Two new `Serializable` message types extending the existing `Request`/`Response` base. **Do not
lock the names**; the *shape* is the design decision — carry one of the three write entries in,
return the leader's committed result (or a clean failure) out.

Two possible shapes (pick one during implementation; both are small):

| Shape | Request carries | Response carries | Note |
|---|---|---|---|
| **Enveloped (tend toward this)** | which op + the matching entry object (`KeyDataInput` / `KeyDataDeleteInput` / `KeyDataExpireInput`) | the committed `RaftLog<…>` (term+index) or an error/enum | One message type per *direction*; op + entry nested inside. Neatest reuse of the three existing payload types. |
| **One-per-op** | a dedicated `KeyDataInput` / `KeyDataDeleteInput` / `KeyDataExpireInput` each | `RaftLog<…>` | Mirrors the codebase's "one type per operation" convention but means three request types + one shared response. |

> The **response** is the same in either shape — a `RaftLog<…>` (committed term/index) **or** a
> small typed failure (no-leader-yet / timed-out / refused). The response type is shared because
> a caller does not care which op it was for the commit result.

### 3.2 The **non-leader → leader** forwarder (the new behavior)

On the node that is *not* the leader, when a write is handed to it:

1. Resolve the leader: `getLeader()` (existing; returns a `RaftNode` at max term, or errors if
   none/ambiguous — same refusal behavior as the facade's `findLeaderNode`).
2. Open / reuse the channel to that leader (existing `ChannelMiddleware#openChannel` / the
   connection machinery in `RaftNodeImpl`), or the already-present outbound connection if one exists.
3. Send the forward request (carrying the entry) and await the correlated response via `NodeRequests`.
4. Relay the leader's result (committed `RaftLog<…>` or the typed failure) back to the caller.

The non-leader **must not** locally `submit`, commit, or apply the entry. It is a pure relay on
the write path; it receives the entry afterward through normal replication. (This is the
correctness core — see §6 invariants.)

### 3.3 The **leader-side** forward handler (the new behavior)

On the leader, when the forward request arrives:

1. Call the *same* `submit(entry)` the in-JVM facade uses (leader-only, today). Do not add any
   commit logic; the leader's existing commit → apply → acknowledge path is exactly what runs.
2. When the `submit` future completes (i.e. after **commit and apply**, per the apply-then-ack
   ordering), build the response with the committed `RaftLog<…>` and send it back.
3. On the known failure modes (election in progress / self not actually leader by the time the
   message lands), respond with a typed failure so the forwarder can re-discover and retry.

### 3.4 Integration seams (the concrete touch points)

- `NodeConnectionOutboundImpl#receive(Object)` — add a branch for the forward request (route to
  the receiver) and for the forward response (`NodeRequests#completeRequest`, exactly like
  `StateResponse`). This is the `// TODO: not very maintainable` dispatch spot — an *additive*
  branch, not a refactor.
- `NodeCommunicationSender` — add a `sendForward(...)` that returns
  `CompletableFuture<ForwardResponse>` (correlated), mirroring `sendStateRequest`.
- `NodeCommunicationReceiver` — add `onForward(sender, req)`; the Leader behavior implements it
  (submit + reply), the Follower/Candidate behavior implements it (forward to leader + relay).
- `RaftNodeBehavior` (Leader / Follower / Candidate) — provide the two handler variants above.

> **Optional (open, not required):** the same `instanceof`-chain registry cleanup noted in Option
> B §3.3. Do not bundle it here; it is separable.

### 3.5 The **caller** interface (where C plugs in)

C is agnostic to *who* calls it. Two concrete integrations, both small and both downstream of C:
- **In-process facade (A's world):** `DistributedCache.put/delete/expire` currently refuse if the
  node is not the leader. With C, "hand the write to *any* in-JVM node; that node forwards
  itself." The facade's `findLeaderNode` refusal becomes either a retryable error or a call to
  the forwarder. This is what makes Option A fault-tolerant to losing the leader's JVM (smaller
  benefit, per A's own §9 — the cache-server JVM is still the entry point).
- **Remote client (B's world):** a non-leader's client-side write handler forwards to the leader
  instead of refusing. This is what makes Option B's "any surviving node is a valid entry point"
  true across leader churn.

---

## 4. The pivotal open questions (C's decisions)

1. **One envelope vs one-per-op.** §3.1 shape decision. Leaning toward the enveloped single
   message type; both are small. Do not over-spend on this.
2. **Connection-reuse strategy.** Does the forwarder open a *new* channel per forward, or reuse
   the already-present outbound `NodeConnectionOutbound` to the leader (the same one `AppendEntries`
   uses)? Reuse is the natural answer (a leader already needs a connection to the follower for
   replication, and a follower to the leader for `VoteRequest`/`PreVote`). Confirm the reuse path
   exists before assuming it (it almost certainly does; the follower already talks to the leader
   during election). Open to record.
3. **No-leader / ambiguity failure shape.** `getLeader()` already refuses on zero or more-than-one
   leaders. The forwarder must turn that into a **retryable** typed error (re-discover and try
   again), not a crash. How long / how often to retry — a policy, not a code detail; leave open.
4. **Timeout on the forward.** `NodeRequests` is currently a hard **5 s** (in-code TODO). A
   forwarded write is a Raft commit under network + load; under pressure it may not fit 5 s.
   Flag: derive the timeout from the same policy as B (§4.6 there), do **not** hardcode 5 s as
   permanent.
5. **Read forwarding — out of scope.** C is write-forwarding. Reads stay local (eventually
   consistent on a follower); if a client wants consistent reads, that is the cross-cutting
   read-index/leases add-on from `REMOTE-CLIENT.md` §4.1, **not** part of C. State this in the
   C docs so a hand-off session doesn't accidentally fold it in.
6. **Forwarding loops.** If the "leader" the forwarder learned about is wrong (a stale
   `getLeader()`), the target might itself forward back. Guard against that: when a leader
   receives a forward but is *not* actually leader by the time it acts, it returns a typed
   failure (not a re-forward). Keep forwarding acyclic.
7. **Audit of `submit` caller context.** `RaftNode#submit` today asserts caller-is-leader. With
   C, the *leader's* call site in the forward handler is leader by construction (the forward
   arrived at the leader). Confirm the assertion still holds at the new call site and that
   nothing else relies on `submit` only being reachable in-process (a quick `grep` should confirm
   that; if the assertion is purely an internal invariant, C is safe).

---

## 5. Acceptance bar (test criteria for a later session; not implementation)

- **Write on a non-leader commits via the leader.** Client/harness does `put` at a node that is
  *not* the current leader; the write commits under the current leader, appears in the leader's
  committed log, and is replicated to all nodes (visible on a follower via its own
  `PartitionMap` shortly after). The caller's response names the leader's committed term/index.
- **No local-then-forward / no divergent copy.** The non-leader does *not* itself append, commit,
  or apply the forwarded entry; exactly one entry per forwarded write exists in the committed log
  (a retried duplicate of the *same* put converges to the same value — record as an acceptable
  property, not a bug).
- **Leader failover mid-forward.** Promote / kill the leader between the client's call and the
  forward's arrival; the non-leader re-discovers the new leader, retries, and the write commits
  under the new leader. The exactly-one-leader invariant holds (no split-brain).
- **No-leader window.** All nodes in election → the forwarder returns a clean, retryable error
  (not a crash) and recovers once a leader is elected.
- **Reads remain eventually-consistent** — assert that, and assert C did **not** silently upgrade
  reads (C is write-forwarding only).
- **Gate:** `mvn -o -B test` green (461/0/0/1 baseline) before committing.

---

## 6. Correctness invariants (must be preserved by any implementation)

- **Apply-then-ack is end-to-end on the leader path.** The *leader* handler must not reply before
  `submit`'s future resolves, and that resolution already happens post-commit + post-apply. The
  *non-leader* must not shortcut by replying from its own `PartitionMap` (which does not contain
  a just-arrived forwarded write until replication lands) and must not locally commit the entry.
- **No split-brain.** One writer per write: the leader does the commit; non-leaders only forward.
  The `submit` leader-assertion (if it exists) must still hold at the forward handler's call
  site.
- **Idempotency of a re-forward.** The cache is last-writer-wins by key; a duplicate forwarded
  put is safe (converges to the same value), but it is still an extra log entry. The design does
  **not** promise exactly-once delivery; it promises *convergence*. Document that explicitly.
- **Forwarding is acyclic.** No node forwards to itself, and no leader re-forwards a forward.
- **Caller does not leak internals.** The caller's contract is `put/delete/expire` → commit
  result / typed failure; it never sees a `RaftNode`, a `RaftLog`, or a cluster address unless
  it *wants* to (that is B's / A's concern, not C's).

---

## 7. Risks & honest trade-offs

- **It assumes a node can reach the leader.** If the network partition is such that the
  forwarder cannot reach the current leader, the write is *stalled* (it cannot commit anyway).
  This is not a regression from today's behavior — today the same node would refuse — but it
  means C does not magically make writes succeed under *arbitrary* network partitions. The
  leader is still required to commit. State this honestly.
- **It adds one more message type to the dispatch chain.** The `instanceof` chain is the
  in-`// TODO` "not very maintainable" spot. An additive branch is small; a registry refactor
  is optional and separable (Option B §3.3).
- **Stale `getLeader()`.** A forwarder that learned the leader from a just-stale state can
  forward to a node that is no longer leader — handled by §4.6 (typed failure, no re-forward
  loop). Confirm this path in the acceptance bar (§5, leader-failover-mid-forward).
- **Positive:** smallest of the three, fully self-contained, composes with both A and B, and
  reuses nearly every existing primitive. It is the natural *first* increment to land if a
  hand-off session wants a quick, shippable win that also unblocks B.

---

## 8. Dependencies & hand-offs

- **Depends on:** nothing new. Builds only on existing, green code.
- **Composes with:**
  - **Option A** (`design/REMOTE-CLIENT-A-EMBEDDED-SHIM.md`) — if A's cache-server JVM still
    hosts a node that can forward, its writes survive losing *leadership*; benefit is smaller
    than for B (the JVM is still the entry point).
  - **Option B** (`design/REMOTE-CLIENT-B-CLIENT-RPC.md`) — if B's client-side write handler
    on a non-leader just calls C's forwarder, B's "any surviving node is a valid entry point"
    guarantee becomes true across leader churn. **C lands before B's B3/B4 (client object and
    failover)**, and B's B5 (consistent-read knob) still sits independently.
- **Cross-cutting (do not fold in by default):** read consistency (read-index/leases) —
  `REMOTE-CLIENT.md` §4.1, **not** part of C; auth model — `REMOTE-CLIENT.md` §4.2, C does
  not address auth (the forward is node↔node over the existing peer channel).

---

## 9. Suggested staging (proposed, not binding)

- **C1** forward request + response types (one of §3.1 shapes) + the sender/receiver seams
  (§3.4). No behavior yet. Keep `mvn -o -B test` green at the end.
- **C2** leader-side handler: on receiving a forward, call `submit` and reply with the committed
  `RaftLog<…>`; typed failures for the known error paths.
- **C3** non-leader forwarder: `getLeader`, open/reuse the connection, send forward, relay
  result; acyclicity guard; typed-retryable error on no-leader.
- **C4** integration test: write on a non-leader commits under the leader; leader failover
  mid-forward recovers; no-leader clean-error; reads remain eventually-consistent. Gate green.

**Sequencing note:** C is the smallest, most self-contained option. A hand-off session should
expect C to be landable **before** B's B3/B4/B5, and A's A2 can pick it up as a "forward if I
am not the leader" branch instead of the current `findLeaderNode` refusal.
