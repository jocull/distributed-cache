# Option A — Embedded shim (cluster in one JVM, expose a thin outer API)

> **Status:** design/spike only — not implemented. Under-specified where implementation will
> discover detail; concrete where the code already fixes the answer. Parent:
> `design/REMOTE-CLIENT.md`.
>
> **One-line:** keep the cluster and its in-JVM `DistributedCache` facade where they are, and
> add a thin, externally-reachable API layer *in that same JVM* that other Java servers call.

---

## 1. High-level view (for a human reviewer)

```
                    outer API (e.g. HTTP/JSON — decision, see §5)
                       [ PUT/GET/DELETE /keys/{key}, /keys/{key}/expire ]
                             ▲            ▲            ▲
                             │            │            │   ordinary network calls
              [app JVM 1]  [app JVM 2]  [app JVM 3]  …      (the "another Java server" goal)

        ┌──────────────────────── "cache server" JVM ─────────────────────────┐
        │  outer API  ──>  DistributedCache (in-JVM facade)  ──>  RaftNode ×3   │
        │  (new, thin)        put/delete/expire/get            (loopback sockets)
        │                                          apply   StateMachineBridge
        │                                          into   PartitionMap (state machine ×3)
        └────────────────────────────────────────────────────────────────────────┘
```

- The **cache cluster still lives inside a single dedicated JVM** (exactly as `Main.java`
  does today: three nodes over loopback sockets, one `RaftManager` bus, three facades).
- A **new, thin layer** — the "shim" — sits in that JVM and delegates to the existing
  `DistributedCache`. This is the only new code, and it is *above* the facade, so it does
  **not** touch the Raft core, the wire protocol, or the dispatch chain.
- Other Java servers talk to the shim over a normal network transport (most naturally
  HTTP/JSON on a local or LAN port).

**Why this is attractive:** it reuses 100% of what is already built and green, adds one
well-isolated layer, and gives you a cross-JVM client *immediately*. Its principal cost is
the **central entry point**: that cache-server JVM is where external writes land (it hosts the
in-JVM leader), so if that JVM dies, external writes are down even though a Raft quorum may
still be alive elsewhere. Option B (and C) are how you remove that centralization.

---

## 2. What already exists that A reuses (no changes)

- `DistributedCache.put/delete/expire/get` — the target surface. Writes complete after
  majority commit **and** local apply (apply-then-ack); reads from the local `PartitionMap`.
- `RaftNode#getLeader()` / `getClusterNodeStates()` — for an optional `/leader` +
  `/cluster/state` observability endpoint.
- `Main.java`'s cluster bring-up — a working reference for the "cache server" JVM.
- The zero-dependency policy (SCOPE #9) — this *constrains* the outer transport choice (§5).

---

## 3. Shape of the shim (proposed, not locked)

A small HTTP-ish API mapping one-to-one onto the facade verbs. **Tentative surface — the exact
verbs, path scheme, and JSON forms are an implementation decision; what matters is that each
maps to a single facade call and the async→response bridging is explicit:**

| Intended operation | Facade call it wraps | Notes |
|---|---|---|
| Write | `cache.put(key, data)` | request `data = Map<String, byte[]>`; response = committed Raft entry (term, index) or a clean error. Bridges the `CompletableFuture` to the transport's completion. |
| Delete | `cache.delete(key)` | no-op-safe (idempotent) in the state machine. |
| Expire | `cache.expire(key, ttl)` | "now" is at apply time, per node — a recorded cache-acceptable choice. |
| Read | `cache.get(key)` | `Optional<KeyDataOutput>` → value+version, or a "not present" response. |
| *(optional)* Leader | `getLeader()` | observability only. |
| *(optional)* Cluster state | `getClusterNodeStates()` | observability only. |

**The one non-trivial bit** is bridging the facade's `CompletableFuture<…>` to the transport
(asynchronous completion) and mapping the known failure modes to responses:
- "no leader currently available (e.g. electing)" → a *retryable* error, not a crash;
- "multiple leaders reported" → a *refuse* error (never guess) — the facade already throws for
  both; the shim just translates these into the transport's error vocabulary;
- timeouts (see §5 on the 5 s request timeout) → a client-visible timeout/error.

**Intentionally left open (decide during implementation):**
- **Which outer transport** — see §5 (this is *the* decision that shapes Option A).
- **Read serving node / consistency** — the shim in this JVM reads from the facade's attached
  node (a leader or a follower depending on wiring). Follower-served reads are eventually
  consistent. A consistent-read policy is a cross-cutting add-on (parent §4.1), **not** part of
  vanilla A unless the shim is deliberately leader-attached for reads.
- **Authentication** — none today; an externally reachable API needs a decision (parent §4.2).
- **Binary encoding of `Map<String, byte[]>`** — base64 (JSON) vs. length-prefixed (raw).

---

## 4. What A does *not* do (scope boundaries, stated to avoid surprises)

- Does **not** add any cache-client message type to the Raft wire (that is Option B).
- Does **not** allow a write to be accepted by a non-leader node and forwarded (that is
  Option C). In A the *shim's* JVM is the write entry.
- Does **not** change Raft semantics, the state machine, or the apply-then-ack ordering.
- Does **not** require the client to know about Raft (it doesn't) — the client only knows keys
  and the endpoint.

---

## 5. The transport decision (the pivotal open question)

The zero-dependency policy (no new libraries) constrains this. Candidates, roughly in order of
policy-fit:

1. **JDK built-in HTTP + JSON** — `com.sun.net.httpserver.HttpServer` (in the JDK, zero-dep)
   with a hand-rolled JSON request/response. Fully zero-dep, easy to debug from `curl`, and
   cross-language. **Likely the default** if we want to stay inside the policy.
2. **Raw socket + the repo's own framing** (4-byte BE length + `Serializable`), i.e. reuse the
   exact frame/codec discipline already in `SocketChannelMiddleware`. Zero-dep, consistent with
   the codebase, but less "plug-and-play" for arbitrary Java clients.
3. **gRPC or similar** — *violates* the zero-dep policy (adds a dependency). Needs an explicit,
   documented exception to SCOPE #9 before it is in scope. Do not assume it is allowed.

**Recommendation for the spike:** treat (1) as the working assumption so the design stays
self-contained and zero-dep, and record (3) as an out-of-scope alternative that requires a
policy exception. Do not burn time designing the HTTP specifics now — the spec's job is that
the shim is a *seam-isolated* layer over the facade with correct async/error mapping.

---

## 6. Acceptance bar (test criteria for a later session; not implementation)

An integration test (real sockets, real process boundary for the *client*) demonstrating:

- **External write → external read:** an out-of-JVM client does `put` then `get` on the same key
  and sees the value (reads from the shim's attached node); delete/expire behave as the facade
  documents (idempotent no-ops where applicable).
- **Retryable no-leader:** trigger an election (e.g. stop/restart a node's leadership in the
  harness) and show the shim returns a retryable error rather than an unhandled crash, and that
  a subsequent call succeeds.
- **Reject-on-ambiguity:** the multiple-leader refusal surfaces as an explicit error, not a
  silent write.
- **Process boundary for the reader:** the *client* in the test is a separate class (and ideally
  a separate JVM) that never holds a `RaftNode` reference — that is the whole point of A.
- **Gate:** `mvn -o -B test` green before committing (461/0/0/1 baseline).

---

## 7. Suggested staging (proposed, not binding)

- **A1** decide the outer transport + a minimal request/response/error contract (documented).
- **A2** the shim: wire the verbs to the facade, bridge `CompletableFuture` → completion, map
  the three known failure modes.
- **A3** integration test with a genuinely external client (separate class / JVM).
- **A4** failure paths (no-leader retry, ambiguity refusal) + (optional) `/leader`,
  `/cluster/state` observability endpoints.

---

## 8. Risks & honest trade-offs

- **Central write entry point.** The cache-server JVM is a single point for external *writes*.
  If it dies, writes are down even if a quorum survives elsewhere. This is the main reason to
  pair A with C (forwarding) or move to B eventually.
- **One more layer to keep honest.** The shim must not drift from the facade's semantics
  (esp. the async/apply-then-ack guarantees). Mitigation: the shim *delegates*; it never
  re-implements commit logic.
- **Security surface.** An exposed port needs the auth decision (parent §4.2) taken before any
  non-trusted-network deployment.
- **Positive:** zero Raft/wire changes, zero new dependencies (with the (1) transport), and the
  fastest credible route to "use the cluster from another Java server."

---

## 9. Dependencies & hand-offs

- **Depends on:** nothing new (builds only on existing, green code).
- **Complements:** C (option-C forwarding would let A's writes survive losing the
  cache-server JVM *if* that JVM still hosts a node that can forward) — though A's
  central-entry design means C's benefit here is smaller than it is for B.
- **Alternative to:** B. If the goal is *only* "give me a cross-JVM client, fast," A is the
  pragmatic choice. If the goal is a *truly distributed, independently-deployable* client
  entry point, B (backed by C) is the end state. See `design/REMOTE-CLIENT-B-CLIENT-RPC.md`
  and `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`.
