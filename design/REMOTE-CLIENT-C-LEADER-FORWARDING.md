# Option C — Non-leader writes: forward, or redirect?

> **Status: DECIDED — Redirect (variant 2) is the chosen mechanism** (choice recorded in §5,
> 2026-11-25). Stages D1–D3 of §7 are the implementation plan. Forward (variant 1) is the
> recorded alternative, deferred behind Redirect.
>
> Historical note: this originally documented the *choice to make at or after B lands*: what happens when a write lands on a
> node that is **not** the leader.
>
> **Two candidate mechanisms are specified in this file:**
>
> 1. **Forward** — the node forwards the write to the current leader in-cluster and relays the
>    leader's commit result back to the client. (The cluster does the routing.)
> 2. **Redirect** — the node replies with a typed "not leader — leader is X" error, and the
>    client re-targets its connection to X and retries the write. (The client does the routing;
>    Redis-style, cf. cluster `MOVED`/`ASK` redirection.)
>
> **Neither is chosen.** B ships without C: its first-cut non-leader write path is a *typed,
> retryable not-leader error* (it may or may not carry the discoverable leader's address — the
> shape is open in B §3.4, and if it does carry one, that *is* the redirect mechanism in variant
> 2 below), and the client re-discovers the leader and retries. Option C is the follow-up
> quality decision on top of that. Do **not** implement either variant before the choice is
> recorded.
>
> Under-specified where implementation will discover detail; concrete where the code already
> fixes the answer. Parent: `design/REMOTE-CLIENT.md`.

---

## 1. Where this sits (for a human reviewer)

B (the chosen design) makes **any surviving node a client entry point**. What B's first cut
deliberately does *not* include is a *handy* path for the "write arrived at a non-leader" case:
today it would be refused, and the client has to re-discover the leader and retry. That works,
but every such write pays for it, and it gets worse during leader churn.

Option C is the decision of *who* fixes that:

```
              client ── write ──>  non-leader node
                                      │
                       ┌──────────────┴──────────────┐
                       ▼                             ▼
             [Redirect]                       [Forward]
   "not leader, leader = X"           node → leader : submit(entry)
   client re-targets to X, retries    leader → node : commit result
   client → X : same write            node → client : commit result
```

The **client-visible outcome is identical** under the two variants (a successful write plus the
leader's committed term/index). What differs is *who makes the second hop*, and therefore what
code that entails:

- **Redirect** adds an *error type* to B's wire plus re-target/retry policy *in the client*.
  The cluster is otherwise untouched.
- **Forward** adds a *request/response pair plus handlers* to the cluster's peer↔peer wire.
  The client is untouched.

Neither changes Raft: in **both** variants **the leader is the only node that commits**,
apply-then-ack is unchanged, and a non-leader never commits, applies, or holds a divergent copy
of the write it is handling.

---

## 2. What already exists that both variants build on (no changes)

- **The exact write entries** — `KeyDataInput` (put), `KeyDataDeleteInput` (delete),
  `KeyDataExpireInput` (expire); all `java.io.Serializable` and the *same objects* the leader
  appends today (`leader.submit(new KeyDataInput(key, data))`, etc., via `DistributedCache`).
- **Leader commit + apply-then-ack** — `RaftNode#submit` →
  `RaftNodeBehaviorLeader#updateCommittedIndex` (majority) → `RaftLogs#commit` (returns logs,
  does **not** ack) → `LogsCommitted` published → `StateMachineBridge` applies to the
  `PartitionMap` (synchronous) → **only then** `RaftLogs#notifyCommitted` completes the
  `CompletableFuture<RaftLog<…>>`. Forward's leader-side handler calls the *same* `submit`; it
  re-implements none of this. The commit result a client ultimately sees under either variant
  is the leader's `RaftLog<op>` (the committed term/index).
- **Leader discovery (in-JVM, already present)** — `RaftNode#getLeader()` →
  `getClusterNodeStates()` → `StateRequest` to active peers → pick `LEADER` at max term.
  Redirect uses this to *name* the leader in the error; Forward uses it to *route* the forward.
- **Request/response correlation (generic — reuse verbatim)** — `Request.requestId` (UUID),
  `Response(Request)` echo, `NodeRequests#getRequestFuture(req, respClass)` +
  `completeRequest(resp)` (a pending-future map keyed by `requestId`; currently a **5 s**
  timeout — in-code TODO "Is this timeout appropriate?"). Forward's node↔node request reuses
  this exactly the way `sendStateRequest`/`StateResponse` does; Redirect does not need it.
- **Transport + dispatch** — `ChannelMiddleware`/`ChannelSide`; `SocketChannelMiddleware` for
  real sockets; `NodeConnectionOutboundImpl#receive(Object)` is the `instanceof` dispatch chain
  (two-phase: pre-introduction vs post-introduction). Forward's message is a **post-introduction**
  one (it travels between already-introduced peers) — the same phase as
  `AppendEntries`/`InstallSnapshot`.

---

## 3. Variant 1 — Forward (the cluster does the routing)

**One-line:** a write that lands on a non-leader is forwarded by that node to the leader, and
the leader's commit result is relayed back to the caller untouched.

### 3.1 A node↔node **forward request + response** pair (in `lib/raft/messages/`)

Two new `Serializable` message types extending the existing `Request`/`Response` base. **Do not
lock the names**; the *shape* is the design decision — carry one of the three write entries in,
return the leader's committed result (or a clean failure) out.

Two possible shapes (pick one during implementation; both are small):

| Shape | Request carries | Response carries | Note |
|---|---|---|---|
| **Enveloped (lean toward this)** | which op + the matching entry object (`KeyDataInput` / `KeyDataDeleteInput` / `KeyDataExpireInput`) | the committed `RaftLog<…>` (term+index) or an error/enum | One message type per *direction*; op + entry nested inside. Neatest reuse of the three existing payload types. |
| **One-per-op** | a dedicated `KeyDataInput` / `KeyDataDeleteInput` / `KeyDataExpireInput` each | `RaftLog<…>` | Mirrors the codebase's "one type per operation" convention but means three request types + one shared response. |

> The **response** is the same in either shape — a `RaftLog<…>` (committed term/index) **or** a
> small typed failure (no-leader-yet / timed-out / refused). The response type is shared because
> a caller does not care which op it was for the commit result.

### 3.2 The **non-leader → leader** forwarder (the new behavior)

On the node that is *not* the leader, when a write is handed to it:

1. Resolve the leader: `getLeader()` (existing; returns a `RaftNode` at max term, or errors if
   none/ambiguous — same refusal behavior as the facade's `findLeaderNode`).
2. Open / reuse the channel to that leader (existing `ChannelMiddleware#openChannel` / the
   connection machinery in `RaftNodeImpl`), or the already-present outbound connection if one
   exists.
3. Send the forward request (carrying the entry) and await the correlated response via `NodeRequests`.
4. Relay the leader's result (committed `RaftLog<…>` or the typed failure) back to the caller.

The non-leader **must not** locally `submit`, commit, or apply the entry. It is a pure relay on
the write path; it receives the entry afterward through normal replication. (This is the
correctness core — see §3.6 invariants.)

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
  (submit + reply); the Follower/Candidate behavior implements it (forward to leader + relay).
- `RaftNodeBehavior` (Leader / Follower / Candidate) — provide the two handler variants above.

> **Optional (open, not required):** the same `instanceof`-chain registry cleanup noted in
> Option B §3.3. Do not bundle it here; it is separable.

**The caller of Forward is B's non-leader write path:** B's write handler on a non-leader node
would invoke this forwarder instead of refusing. A (the in-process shim) is rejected — there is
no separate in-jVM caller path to wire in.

### 3.5 Pivotal open questions specific to Forward

1. **One envelope vs one-per-op.** §3.1 shape decision. Lean toward the enveloped single
   message type; both are small. Do not over-spend on this.
2. **Connection-reuse strategy.** Does the forwarder open a *new* channel per forward, or reuse
   the already-present outbound `NodeConnectionOutbound` to the leader (the same one
   `AppendEntries` uses)? Reuse is the natural answer (a leader already needs a connection to
   the follower for replication, and a follower to the leader for `VoteRequest`/`PreVote`).
   Confirm the reuse path exists before assuming it (it almost certainly does; the follower
   already talks to the leader during election). Open to record.
3. **No-leader / ambiguity failure shape.** `getLeader()` already refuses on zero or more-than-one
   leaders. The forwarder must turn that into a **retryable** typed error (re-discover and try
   again), not a crash. How long / how often to retry — a policy, not a code detail; leave open.
4. **Timeout on the forward.** `NodeRequests` is currently a hard **5 s** (in-code TODO). A
   forwarded write is a Raft commit under network + load; under pressure it may not fit 5 s.
   Flag: derive the timeout from the same policy as B's client-side timeout (B's §4.6 there), do
   **not** hardcode 5 s as permanent.
5. **Forwarding loops.** If the "leader" the forwarder learned about is wrong (a stale
   `getLeader()`), the target might itself forward back. Guard against that: when a leader
   receives a forward but is *not* actually leader by the time it acts, it returns a typed
   failure (not a re-forward). Keep forwarding acyclic.
6. **Audit of `submit` caller context.** `RaftNode#submit` today asserts caller-is-leader. With
   Forward, the *leader's* call site in the forward handler is leader by construction (the
   forward arrived at the leader). Confirm the assertion still holds at the new call site and
   that nothing else relies on `submit` only being reachable in-process (a quick `grep` should
   confirm that; if the assertion is purely an internal invariant, Forward is safe).

### 3.6 Invariants (Forward)

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
  result / typed failure; it never sees a `RaftNode` or `RaftLog` unless it *wants* to (B's
  client may surface the term/index as a detail).

### 3.7 Acceptance bar (test criteria for a later session; not implementation)

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
- **Reads remain eventually-consistent** — assert that, and assert Forward did **not** silently
  upgrade reads (Forward is write-forwarding only).
- **Gate:** `mvn -o -B test` green (461/0/0/1 baseline) before committing.

### 3.8 Risks (Forward)

- **It assumes a node can reach the leader.** If the network partition is such that the
  forwarder cannot reach the current leader, the write is *stalled* (it cannot commit anyway).
  This is not a regression from today's behavior — today the same node would refuse — but it
  means Forward does not magically make writes succeed under *arbitrary* network partitions. The
  leader is still required to commit. State this honestly.
- **It adds one more message type to the dispatch chain.** The `instanceof` chain is the
  in-code `// TODO` "not very maintainable" spot. An additive branch is small; a registry
  refactor is optional and separable (Option B §3.3).
- **Stale `getLeader()`.** A forwarder that learned the leader from a just-stale state can
  forward to a node that is no longer leader — handled by §3.5.5 (typed failure, no re-forward
  loop). Confirm this path in the acceptance bar (§3.7, leader-failover-mid-forward).
- **Positive:** the cluster-side variant of C — it reuses nearly every existing primitive
  (`submit`, `getLeader`, `NodeRequests`, the dispatch seam), and the client stays simple (fire
  and wait).

---

## 4. Variant 2 — Redirect (the client does the routing; Redis-style)

**One-line:** a write that lands on a non-leader is refused with a *typed* error that **names the
current leader**, and the client re-targets its connection to that leader and retries the same
write. The cluster stays dumb; the client does the routing. (Cf. Redis-cluster `MOVED`/`ASK`
redirection; a Redis master's `READONLY` refusal on replica writes is a close relative.)

### 4.1 Shape

- On a non-leader, B's write handler (*the same handler B already builds* — B §3.4) does **not**
  `submit`; instead it resolves `getLeader()`:
  - **one leader found** → the response is the typed *"not leader"* error **carrying the
    leader's `NodeAddress`/node id**;
  - **none or more than one** → the response is the typed *"no leader right now"* error
    **without** a leader address (purely retryable — B's first-cut behavior).
- The client (B's `RemoteCacheClient`) treats the error as "re-target": refreshes its leader
  view with the named address, (re)opens a connection to the named leader, and retries the
  *same* write, bounded-retry + backoff (policy, open).
- Side benefit: the error is a continuous *leader hint* — the client's leader view stays warm
  even if it never explicitly asked for leadership.

### 4.2 Integration seams (small)

- **Wire:** one new response type, **or** one new error field/enum on B's existing
  `CacheWriteResponse` (name open; the shape — "typed error + an optional leader address" — is
  the decision). This rides **B's client-facing wire**; it adds **no new cluster↔cluster
  message**.
- **Node-side:** B's non-leader write handler (B §3.4) returns the error instead of today's
  no-leader refusal. No new `RaftNodeBehavior` handler, no `NodeRequests` correlation, and no
  change to the dispatch chain (unless the error is modeled as a new message type — the open
  shape choice from §4.3.1).
- **Client-side:** B's `RemoteCacheClient` gains the re-target/retry loop on "not leader"
  (bounded retries, re-discovery, anti-loop — §4.3.2/§4.3.3).
- **Auth:** none beyond B's client-facing auth decision (same surface).

### 4.3 Pivotal open questions specific to Redirect

1. **Error shape.** New response type vs. a field on `CacheWriteResponse` (§4.2 shape choice).
   Small; decide it and record it.
2. **Retry policy.** Bounded attempts, backoff, and the treatment of "leader changed again"
   between the error and the retry (the answer is that the same error is retryable and the
   client re-discovers; a third consecutive not-leader response naming the node it was told to
   go to is a hard error / re-discovery — anti-loop; policy detail open).
3. **Stale leader hints.** The named leader may be demoted again before the client's retry lands.
   The error must **always** stay retryable, and the client must re-discover on a second
   not-leader response (covered by §4.3.2 anti-loop).
4. **Client-side latency.** Under heavy leader churn, every write pays an extra
   error→retry round trip at the *client*. Acceptable trade if we take this variant (it is
   exactly how Redis-cluster clients run); record it.

### 4.4 Invariants (Redirect)

- **Writes commit only on the leader** — unchanged; the redirect is a re-target, not a re-route.
  The non-leader never commits or applies the entry locally.
- **The error is *always* retryable** — zero or multiple leaders → the error carries no leader
  hint and the client re-discovers. No crash path over the wire.
- **Bounded client retries / anti-loop** — the client has bounded attempts + re-discovery
  (policy, open); no infinite redirect loops.
- **Idempotency of retried puts** — same as Forward: last-writer-wins by key, convergence; a
  retried put may duplicate a log entry (a safe, documented property).
- **Caller contract unchanged** — the client still sees `put/delete/expire` → commit result /
  typed failure; "not leader" is one more error to translate, not a new success path.

### 4.5 Acceptance bar (test criteria for a later session; not implementation)

- **Write on a non-leader succeeds end-to-end.** Client does `put` that lands on a non-leader
  node; the client sees the typed "not leader — leader=X" error, re-targets to X, and the same
  write commits under the leader (the client's response names the leader's committed term/index).
  Replicated to all nodes.
- **Leader churn mid-redirect.** The named leader is demoted before the client's retry lands; the
  client gets another "not leader" error (now naming the new leader, if discoverable),
  re-discovers, retries, and the write commits under the finally-elected leader. The
  exactly-one-leader invariant holds (no split-brain).
- **No-leader window.** All nodes in election → the error carries no leader, the client backs
  off/retries; it recovers when a leader exists.
- **Anti-loop.** A scripted loop (each node says not leader, naming a different node) terminates
  in a clean client error under the bounded-retry policy, not an infinite loop.
- **Reads untouched.** Reads stay local-serving (eventually consistent on a follower); Redirect
  is **write-only** scope.
- **Gate:** `mvn -o -B test` green (461/0/0/1 baseline) before committing.

### 4.6 Risks (Redirect)

- **The client pays the second hop.** Under heavy leader churn, every write takes the
  error→retry path and write latency from the client's perspective rises. That is the real,
  known cost — the offsetting benefit is that the *cluster* gains nothing new to maintain.
- **Complexity moves into the client.** Re-target/retry/anti-loop lives in `RemoteCacheClient`,
  and every client implementation must have it (today: one). This is the *cost* of variant 2 and
  the *benefit* of variant 1.
- **Stale leader hints** — handled by the always-retryable + bounded re-discovery rules
  (§4.3.2/§4.3.3).
- **Positive:** zero new cluster↔cluster messages; zero new `RaftNodeBehavior` handlers; zero
  `NodeRequests` correlation; the smallest *cluster-side* footprint of both variants — and it
  doubles as continuous leader-tracking for the client.

---

## 5. The choice — comparing the two

> **DECISION — 2026-11-25 (redirect chosen).**
>
> **Variant 2 — Redirect** is the chosen mechanism for the non-leader write path. The node
> refuses the write with the typed not-leader error **carrying the leader's `NodeAddress`**
> when one is discoverable; the client (B's `RemoteCacheClient`) re-targets directly to the
> named leader and retries the same write (bounded, with the re-discovery / anti-loop policy of
> §4.3.2/§4.3.3 applied). No new cluster↔cluster messages, no new `RaftNodeBehavior` handlers,
> no `NodeRequests` correlation — the cluster-side change is limited to B's existing non-leader
> write handler returning the leader-naming error. The continuous leader-hint side effect of
> §4.1 is accepted as a benefit. Forward (variant 1) is **not** rejected outright — it remains
> the recorded alternative if measured client-side latency under leader churn proves
> unacceptable — but it is deferred behind Redirect. Stages D1–D3 of §7 are the implementation
> plan.
>
> *Reasoning recorded at the time of the choice:* B's client already owns the NOT_LEADER retry
> seam (one retry per B §3.5); Redirect extends the existing typed error with the leader's
> address and gives the client a direct re-target rather than a full probe sweep. This keeps the
> cluster footprint minimal (frozen codec surface, the `instanceof` dispatch chain left
> untouched) and concentrates the new behavior in the single existing client implementation.

| | **Forward** (variant 1) | **Redirect** (variant 2) |
|---|---|---|
| Who fixes the second hop | **Cluster** (node → leader) | **Client** (node names the leader) |
| New cluster↔cluster messages | **2** (forward request + response) | **0** (a typed error on B's existing wire) |
| New `RaftNodeBehavior` handlers | **2** (Leader: submit+reply; Follower: getLeader+forward) | **0** (B's existing non-leader write handler returns the error) |
| `NodeRequests` correlation | Yes (the in-cluster forward reuses it) | No |
| Client complexity added | **Low** (fire and wait) | **Higher** (re-target / retry / anti-loop) |
| Write latency under churn | One cluster hop (node→leader) | One client round-trip (error + retry) |
| Cluster-side footprint | **Medium** (one message pair + two handlers + acyclicity guard) | **Small** (one error type, or one field on an existing response) |
| Loop / acyclicity risk | Must guard cluster loops (stale leader) | None in-cluster; anti-loop is client-side policy |
| Leader-tracking side effect | None (the cluster absorbs it) | **Continuous leader hints to the client** |
| Resembles | In-process "forward to coordinator" | Redis-cluster `MOVED`/`ASK` |

**Position, deliberately open.** Neither variant is in the B first cut. Come back to this when
B is running (or tested) and we can *measure* how much the first-cut retryable-no-leader error
costs, and make the choice on: (a) expected leader-churn profile, (b) whether we *want*
client-side leader tracking (Redirect gives it free), (c) the cluster-side complexity budget,
(d) write-latency tolerance under churn.

**Record the choice in this file (or a successor) before implementing either variant.**
Option A (the in-process shim) is rejected — B's non-leader write path is the *only* remaining
caller of either variant.

---

## 6. Cross-cutting (both variants)

- **Read consistency — out of scope for both.** Both variants are the *write* path; reads
  remain local-serving (eventually consistent on a follower). A consistent-read mechanism
  (read-index / leases), if ever wanted, is the cross-cutting item in `REMOTE-CLIENT.md` §4.1,
  **not** part of C — a hand-off session must not fold it in.
- **Auth.** Forward rides the existing peer↔peer channel (peer auth, not client auth); Redirect
  is a new error type on B's client-facing surface (so B's auth decision applies).
- **Timeout.** Forward's in-cluster request hits the `NodeRequests` 5 s TODO (§3.5.4); Redirect
  hits B's client-side timeout (B §4.6). Whichever we pick, do **not** silently hardcode 5 s.
- **Zero-dependency.** Both variants stay zero-dep by construction (existing frame, existing
  codec).

---

## 7. Suggested staging (once the choice is a go)

**If Forward is chosen:**

- **C1** forward request + response types (one of §3.1's shapes) + the sender/receiver seams
  (§3.4). No behavior yet. Keep `mvn -o -B test` green at the end.
- **C2** leader-side handler: on receiving a forward, call `submit` and reply with the committed
  `RaftLog<…>`; typed failures on the known error paths.
- **C3** non-leader forwarder: `getLeader`, open/reuse the connection, send the forward, relay
  the result; the acyclicity guard; the typed-retryable no-leader error.
- **C4** integration test: write on a non-leader commits under the leader; leader failover
  mid-forward recovers; no-leader clean error; reads remain eventually-consistent. Gate green.

**If Redirect is chosen:**

- **D1** error shape decision (§4.3.1) + wire it into B's non-leader write handler (B §3.4),
  naming the leader address when known. Keep `mvn -o -B test` green at the end.
- **D2** `RemoteCacheClient`: the re-target/retry + anti-loop on "not leader" (§4.3.2/§4.3.3).
- **D3** integration test: the §4.5 acceptance bar (non-leader write succeeds; leader-churn
  convergence; no-leader clean error; anti-loop; reads unchanged). Gate green.

**Sequencing:** neither C-nor-D staging changes B's B1–B5 (see B §6); it lands **after** B's
client exists and **after** the choice in §5 is recorded.
