# Option B — Client RPC on the wire (the CHOSEN design)

> **Status:** ✅ **The CHOSEN design — the spec to implement** (decision 2026-11-24). Not yet
> implemented. A was considered and is **rejected & archived** — `design/REMOTE-CLIENT-A-EMBEDDED-SHIM.md`
> keeps the record and the why; C is **deferred** — the open forward-vs-redirect (Redis-style)
> decision for non-leader writes, in `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`. Under-
> specified where implementation will discover detail; concrete where the code already fixes the
> answer. Parent: `design/REMOTE-CLIENT.md`.
>
> **One-line:** put the cache-client operations on the existing wire protocol so **any node can
> accept a client** connection, and build a thin `RemoteCacheClient` (a separate JVM) that
> discovers the leader and routes `put/get/delete/expire` over sockets. This removes the
> "must be in the leader's JVM, must be in the same JVM at all" constraint.

---

## 1. High-level view (for a human reviewer)

```
   [client JVM]  —  no RaftNode objects, no in-JVM references —  (the "another Java server" goal)
        │
        │   1) discover:  "who is the leader?"        (reuse StateRequest / leader discovery)
        │   2) ops:        put / get / delete / expire over the existing frame + protobuf codec (2026-11-25)
        ▼
   ┌──────────── any node ────────────┐
   │ not leader? ──> FIRST CUT: typed retryable "not leader" error — client
   │               re-discovers the leader and retries   (deferred Option C:
   │               cluster forwards it in-cluster, OR the error names the leader and the client re-targets)
   │ leader?       ──> submit (existing) → commit → apply ──> CacheWriteResponse
   │ read:         ──> serve from local (or leader) PartitionMap ──> CacheReadResponse
   └───────────────────────────────────┘
```

- **Any node is a valid client entry point.** The client picks a node (bootstrap list), asks the
  cluster for its state, and routes. First cut: a write that lands on a non-leader gets a *typed,
  retryable "not leader" error* and the client re-discovers the leader and retries. The follow-up
  (Option C, **deferred** — forward vs. redirect, undecided) either has the cluster *forward*
  that write to the leader, or the error *names the leader* and the client re-targets to it.
  Either way, surviving nodes keep serving clients even when the current leader — or the leader's
  co-located JVM — is gone.
- **It reuses the existing wire** — same 4-byte-BE-length frame, the payload codec having
  been Protocol Buffers since the 2026-11-25 migration (`NETWORKING.md` §2.1; the §3.2
  addendum below), same `ChannelMiddleware`/`ChannelSide`, same `NodeRequests`
  request/response correlation, same `instanceof` dispatch chain (`NodeConnectionOutboundImpl`).
- **A new client-side object** (`RemoteCacheClient`, name tentative) lives in the client JVM and
  exposes the same verbs as `DistributedCache` but over the network.

**Why it's the CHOSEN design:** it is the only option that makes the cache cluster a genuinely
*externally-reachable, distributed service* with no central JVM (A — the central-JVM route — was
considered and rejected; see its archived record). **Why it's the largest:** it
touches the dispatch chain (`// TODO: not very maintainable` in-code), needs a decision on how a
*client* connection is distinguished from a *Raft-peer* connection, and must fix the
non-`Serializable` read payload (now resolved — see the §3.2 addendum). Each of those is called
out below with honest open questions.

---

## 2. What already exists that B reuses (no changes)

- **Frame + codec** (`NETWORKING.md` §2.1, D8/#9) — the 4-byte-BE-length framing is untouched;
  the payload codec has been Protocol Buffers since the 2026-11-25 migration
  (`SCOPE.md` Follow-up #11 — **done**; the one-off new-dependency exception was approved by the
  user 2026-11-24). The old JDK-serialization basis (Decision #9) that made the codec a
  PoC-grade one — offline-constraint-driven, not cross-version-safe, unusable by non-JVM
  clients — is what the migration replaced, via that decision's own recorded escape-valve path.
  Consequence for B (binding): its new messages must keep **flat, language-portable payloads**
  (string / byte[] / long, wrapping the `KeyData*` inputs) rather than nested Java object
  graphs, and they are born straight into the existing codec — either as new `WireMessage`
  envelope variants with fresh protobuf field numbers (1–14 are frozen, never re-used), or as
  dedicated messages with named encode/decode like `KeyDataOutput` rather than re-opening an
  existing one; a non-JVM client can read the discriminator and payloads without loading a JVM
  class, a property the old basis lacked.
- **`Request`/`Response` correlation** — `Request.requestId` (UUID), `Response(Request)` echo,
  `NodeRequests#getRequestFuture/completeRequest` (a generic pending-future map; currently a
  **5 s** timeout — in-code TODO). B's client requests reuse this **verbatim** (same pattern as
  `sendStateRequest`/`StateResponse`).
- **Cache operation payloads (write side)** — `KeyDataInput`, `KeyDataDeleteInput`,
  `KeyDataExpireInput` (all `Serializable`). These are *already* the exact log entries the leader
  appends — so a forwarded/submitted write is the same object as today.
- **Leader submit + apply-then-ack** (`RaftNode#submit`, `RaftNodeBehaviorLeader`,
  `StateMachineBridge`) — B does **not** re-implement any of this; node-side handler for a write
  ultimately calls the same `submit` the in-JVM facade already calls.
- **Leader discovery primitives** — `RaftNode#getLeader()`/`getClusterNodeStates()` in-JVM; the
  networked form reuses `StateRequest` (see §4, discovery).
- **Transport** — `ChannelMiddleware`/`SocketChannelMiddleware`. The client opens a channel to a
  node the same way a node opens one to a peer.

---

## 3. New pieces (proposed; names tentative)

### 3.1 Wire message types (in `lib/raft/messages/`, extend `Request`/`Response`)
Follow the codebase's existing **one-type-per-operation** convention rather than a single
discriminated type. **Tentative:**

| Purpose | Tentative request | Tentative response | Carries |
|---|---|---|---|
| Write (put) | e.g. `CachePutRequest` | e.g. `CacheWriteResponse` | a `KeyDataInput` (already Serializable) |
| Delete | e.g. `CacheDeleteRequest` | `CacheWriteResponse` | a `KeyDataDeleteInput` |
| Expire | e.g. `CacheExpireRequest` | `CacheWriteResponse` | a `KeyDataExpireInput` |
| Read (get) | e.g. `CacheReadRequest` | e.g. `CacheReadResponse` | a key in; value + version out |
| *(optional)* state/leader discovery | *(reuse `StateRequest`/`StateResponse`)* | | |

> **Do not lock the names.** The *shape* (per-operation request, shared write response,
> read request/response) is the design decision; names and exact payload nesting are
> implementation detail and may change.

### 3.2 The **read-payload serializability fix** (required, flag prominently)
`KeyDataOutput` (the `get` return type) is **not** `Serializable`. B's `CacheReadResponse` must
carry the value + version across the wire, so implementation must **either** make
`KeyDataOutput` implement `Serializable` (with `serialVersionUID`, matching the house style of
its siblings) **or** introduce a dedicated serializable read-response payload. This is a *small but
real* change to a type in `lib/communication/` — flag it to any session so it isn't missed, and
pick one approach and record the choice. (Both are low-risk; the decision affects whether
`KeyDataOutput`'s API surface changes.)

> **Addendum (2026-11-25, resolved by the Task 1 codec migration — commits `94ce636`/`1780c32`):**
> this debt is already closed. `KeyDataOutput` is a dedicated message in
> `src/main/proto/wire.proto`, and `WireCodec` carries it with named encode and
> `decodeKeyDataOutput` — so `CacheReadResponse`'s value + version cross the wire as proto bytes
> without `KeyDataOutput` gaining `Serializable` or a new payload type (no API-surface change at
> all). The §3.1 wire message types are born straight into the same codec and IDL. Record here so
> whoever stages B does not re-open the question; nothing above is deleted or renumbered.

### 3.3 Dispatch / sender / receiver seams (the concrete integration points)
Adding a message type means touching the existing chain — this is the `// TODO: not very
maintainable` spot. The *minimal* change is to add branches; the *opportunity* is an optional
registry refactor. **The spec mandates the minimal change and lists the refactor as an open
option, not a requirement:**

- `NodeConnectionOutboundImpl#receive(Object)` — add a branch per new request/response type.
  (Responses: `CacheWriteResponse`/`CacheReadResponse` → `requests.completeRequest(…)` exactly
  like `StateResponse`. Requests: route to a `NodeCommunicationReceiver#onX`.)
- `NodeCommunicationSender` — add `sendCachePut(...)/sendCacheDelete(...)/sendCacheExpire(...)`
  each returning `CompletableFuture<CacheWriteResponse>` (correlated, mirroring
  `sendStateRequest`), and `sendCacheRead(...)` → `CompletableFuture<CacheReadResponse>`.
- `NodeCommunicationReceiver` — add `onCachePut/onCacheDelete/onCacheExpire(sender, req)` and
  `onCacheRead(sender, req)`; `RaftNodeBehavior` (Leader/Follower/Candidate) provides handlers.
- **Optional (open):** replace the `instanceof` chain with a `Map<Class<?>, BiFunction<…>>`
  registry to retire the TODO. *Do not require it*; it is a clean-up that can ride along or be a
  separate commit.

### 3.4 Node-side handlers (behavior)
- **Write on a leader** → call the same `submit(entry)` the in-JVM path uses (commit → apply →
  acknowledge), return the committed entry in `CacheWriteResponse`. **No new commit logic.**
- **Write on a non-leader** → first cut (B ships this): do **not** submit locally; reply with a
  typed **retryable "not leader" error** (shape open; if a leader is discoverable it may carry the
  leader's address as a hint — that hint *is* C's redirect mechanism). The client re-discovers
  the leader and retries (the §3.5 retry behavior already covers this case).
- **Write on a non-leader — the deferred Option C on top of this** → either the cluster
  *forwards* the write to the leader in-cluster and relays the leader's commit result back (C
  variant 1), or the error above *names the leader* and the client re-targets its connection and
  retries (C variant 2, *redirect*, Redis-style). The choice is in
  `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`; **neither variant is chosen yet** — implement B
  with the first-cut error and attach C only when (and if) the choice is recorded.
- **Read** → serve from the node's `PartitionMap` (eventually consistent on a follower) **or**
  route to the leader / add a consistent-read policy (cross-cutting, parent §4.1). The simplest
  first cut is local serving; record it.

### 3.5 The client object (in the client JVM)
A `RemoteCacheClient` (tentative name) configured with a **bootstrap node list**
(`NodeAddress`es), that:
- discovers the leader (reuses `StateRequest`/leader discovery, §4),
- exposes `put/delete/expire → CompletableFuture<…>` and `get → Optional<…>` or a future,
- retries/re-discovers on leader loss, on no-leader (election in progress), and on the
  first-cut "not leader" write error (§3.4); if C's *redirect* variant is later chosen, the same
  loop re-targets to the address the error named (bounded retries + anti-loop — policy, see the
  C spec).
- **Must not** hold or require any `RaftNode` reference. That is the whole point.

---

## 4. The pivotal open questions (B's decisions — flag, do not silently choose)

> **Status (2026-11-25):** all six decisions are resolved and recorded under each question
> below; the B first cut is built against them.

1. **How is a *client* connection distinguished from a *Raft-peer* connection?**
   Both would arrive via `RaftNode#acceptConnection` → `onNewConnection` → a fresh
   `NodeConnectionOutbound`. A Raft peer performs `Introduction` (establishing `remoteNodeId`);
   a **client is not a Raft node** and must not. Options (open):
   - a lightweight **client handshake / hello** message as the first frame (then the connection is
     tagged "client"); or
   - a **separate listener / port** for clients vs. Raft peers; or
   - the client messages are admitted in the **pre-introduction** dispatch phase (like
     `StateRequest`), so they never require `remoteNodeId`.
   **This is the foundational B decision.** The spec deliberately leaves it open because the
   answer drives the auth model (§4.2) and the connection lifecycle.
   **Decision (2026-11-25): pre-introduction dispatch, single port.** The connection is
   distinguished by its *first frame*: a Raft peer's first frame is `Introduction` (which
   establishes `remoteNodeId`); a client's first frame is a client verb, served from the
   pre-introduction dispatch phase beside `StateRequest` — it never requires `remoteNodeId`.
   No extra client handshake message, no second port/listener. Cross-checked against Redis at
   the user's request: Redis also multiplexes clients and replicas on the single data port,
   first message determining the role (replicas speak the internal `REPLCONF` protocol;
   clients send commands); its only additional port is the admin channel, which clients
   never use; cluster clients learn the full node set from any node (`CLUSTER SLOTS`).
2. **Leader discovery shape.** Does the client send `StateRequest` to a bootstrap node and have
   *that node* relay to peers? Do clients bootstrap with a full node list (like
   `InMemoryTopologyDiscovery` does)? What is the failover/retry/backoff when "no leader
   currently available"? Open (mirror the facade's `IllegalStateException` refusal behavior as
   the *error*, and add bounded retry as the *policy*).
   **Decision (2026-11-25): hybrid topology discovery, via an extended `StateResponse`.**
   Each node shares its cluster view by adding ONE optional `cluster` (repeated
   `NodeAddress`) field to the existing `StateResponse` — an additive proto field (next
   free number, 7), not a new envelope variant; the frozen 14-variant closed set and all
   existing peer traffic are untouched. Client loop: (a) probe the bootstrap nodes (they
   may be a partial list), (b) UNION the cluster views they report, (c) fan out
   `StateRequest` to that union, (d) pick the LEADER (highest term). No leader ⇒ clean,
   typed, retryable error + bounded client backoff.
3. **Read consistency policy.** Local-serving (eventually consistent) vs. leader-routed vs.
   read-index/leases (parent §4.1). Recommend: **local-serving first, document it, add a
   `consistency=` knob later** — do not couple B's first cut to the consistent-read design.
   **Decision (2026-11-25): local-serving reads first** (eventually consistent — the same
   semantics in-JVM `DistributedCache#get` already has), documented as such; a
   `consistency=` knob / read-index remains a follow-up, not part of B's first cut.
4. **Authentication / authorization.** None exists. A client listener is externally reachable →
   an auth decision is required before trusted-network deployment (parent §4.2). Open; the
   handshake decision (§4.1) is the natural place to attach it.
   **Decision (2026-11-25): no auth in v1** — B's first cut ships without authentication
   (trusted-network assumption, documented). The pre-introduction dispatch zone (decision 1)
   is recorded as the SEAM where authentication attaches later; the §5 "unknown client is
   refused cleanly" acceptance item applies when that lands.
5. **Write idempotency / dedup on retry.** A failed-then-retried write may re-submit the same
   client intent → two log entries. In this cache `put` is last-writer-wins by key, so a
   double-forward of the *same* put converges to the same value (record this as a **beneficial
   property**, not a bug). Confirm the semantics rather than adding a dedup table.
   **Decision (2026-11-25): LWW convergence, no dedup table.** Semantics confirmed: the
   cache is last-writer-wins by key — a failed-then-retried write re-applies the same
   key→value, so the state converges to the same value; `delete`/`expire` on an absent key
   is a no-op. No dedup table is added; this is recorded as a documented beneficial property
   of the wire layer.
6. **Timeout.** `NodeRequests` is currently a hard **5 s** (in-code TODO "Is this appropriate?").
   A remote write under load should not be assumed to fit 5 s. Flag: make the timeout
   configurable or derive it per request class; *do not* hardcode 5 s as permanent.
   **Decision (2026-11-25): per-request timeout on B's sender seams.** The client-request
   path (B's sender/receiver seams) uses a per-request, client-configurable timeout with a
   **10 s default**. The existing 5 s hard timeout in `NodeRequests` and its in-code TODO
   stay untouched for the 12 Raft traffic types — that question is out of B's scope.

---

## 5. Acceptance bar (test criteria; not implementation)

- **Genuine remote client:** an in-*separate-JVM* (or at minimum no-`RaftNode`-reference) client
  does `put` → `get` → `expire` → `delete` over real sockets and observes the documented
  semantics, including the write-durability guarantee (committed by majority and applied before
  the future completes).
- **Read-your-own-write on the leader** holds (apply-then-ack is preserved through the wire).
- **Read-after-write on a different node** converges (assert eventual consistency, *not*
  linearizability, unless §4.3 is later chosen).
- **Leader failover:** kill/depromote the leader; the client re-discovers and a subsequent write
  commits under the new leader; exactly-one-leader invariant holds (no split-brain write).
- **No-leader window:** all nodes in election → the client gets a clean, retryable error (not a
  crash), and recovers when a leader exists.
- **Unknown client:** if §4.4 auth is added, an unauthenticated client is refused cleanly.
- **Gate:** `mvn -o -B test` green (464/0/0/1 baseline as of 2026-11-25) before committing.

---

## 6. Suggested staging (proposed, not binding)

- **B1** message types (born into the protobuf codec — §3.2 addendum) + the read-payload
  wireability (already done via the codec migration — §3.2 addendum) + dispatch/sender/receiver
  seams (§3.3). No client yet. Keep `mvn -o -B test` green at the end.
- **B2** node-side **write** handler on a leader (reuse `submit`), plus the read handler
  (local-serving). Wire the correlation futures.
- **B3** `RemoteCacheClient` object + the client-connection distinction decision (§4.1).
- **B4** leader discovery + failover + no-leader error path + timeout policy (§4.2/§4.6).
- **B5 (optional)** consistent-read knob / read-index (parent §4.1), and/or the dispatch
  registry refactor (§3.3 optional).

**Sequencing note:** B ships with the first-cut non-leader error (§3.4) — "any node is an entry
point" holds in the weak sense (writes still succeed, via the client re-discovering the leader).
Option C — the deferred decision of whether the cluster *forwards* non-leader writes (variant 1)
or the error *names the leader* and the client *re-targets* (variant 2) — lands **after B**, and
only after `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md` records which variant was chosen. C
blocks nothing in B1–B5.

---

## 7. Risks & honest trade-offs

- **Touches the dispatch chain** — the exact spot the in-code TODO calls "not very maintainable."
  Mitigation: minimal additive change (new branches), registry refactor optional and separable.
- **The client-vs-peer distinction is foundational** and interacts with auth + connection
  lifecycle; a wrong early choice is costly. Mitigation: decide it first (B3) and record it.
- **Security surface** — an externally reachable client listener is a real auth decision (§4.4).
- **Positive:** this is the chosen design precisely because it is the only option that makes the
  cluster a genuinely externally-reachable distributed service with any surviving node as an
  entry point — the stated end goal ("leverage the cache cluster from another Java server").
  The wire's one new dependency is the user-approved one-off (protobuf, the 2026-11-24 exception,
  `SCOPE.md` Follow-up #11) — B itself adds no further.

---

## 8. Dependencies & hand-offs

- **Depends on:** the read-payload wireability — **done** (the 2026-11-25 codec migration closed
  it; §3.2 addendum). Nothing else.
- **Option C (DEFERRED — decision pending):**
  `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md` — what a non-leader does with a client write:
  *forward* it in-cluster to the leader (variant 1) vs. *redirect* — a typed "not leader —
  leader is X" error and the client re-targets (variant 2, Redis-style). B's first-cut §3.4
  error path is the attachment point: under *forward*, the node-side handler calls C's forwarder
  instead of erroring; under *redirect*, B's not-leader error *is* the mechanism (plus the
  client re-target loop). Neither variant is chosen — do not implement either before the choice
  is recorded in the C spec.
- **Option A (REJECTED — archived):** `design/REMOTE-CLIENT-A-EMBEDDED-SHIM.md` — no dependency
  either way; the file is kept as the record of *why A was not pursued* (central write entry
  point). Not a live alternative to B.
- **Cross-cutting (do not fold in by default):** read consistency (read-index/leases), auth model
  — parent §4.
