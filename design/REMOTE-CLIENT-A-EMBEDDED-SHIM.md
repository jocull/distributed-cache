# Option A — Embedded shim (REJECTED — the record of the choice is kept)

> **Status: REJECTED (2026-11-24). Not being pursued. Do not implement.**
>
> This file is kept so a future session or human reviewer can see *why this project does not
> have a dedicated "cache server" JVM and no outer HTTP-style API. The chosen route to the same
> goal ("use the cache cluster from another Java server") is **Option B** — client RPC on the
> existing wire — see `design/REMOTE-CLIENT-B-CLIENT-RPC.md` for the spec being implemented.
>
> The full original A specification (shim shape, transport decision, acceptance bar, staging) is
> in git history: `git show 7d6dbea:design/REMOTE-CLIENT-A-EMBEDDED-SHIM.md`. If the decision
> is ever reversed, start from that file and re-derive it against the current tree.

---

## 1. What it was (compact restatement)

Run the whole cache cluster in a **single dedicated "cache server" JVM** (exactly as
`Main.java` does today: three nodes over loopback), and add a thin, externally-reachable API
layer *inside that same JVM* — most naturally HTTP/JSON via `com.sun.net.httpserver` to stay
zero-dep — that delegates to the in-JVM `DistributedCache` facade:

```
            outer API (HTTP/JSON)
               ▲      ▲      ▲             ordinary network calls
 [app JVM 1]  [app JVM 2]  [app JVM 3]    (the "another Java server" goal)

     ┌──────────────── "cache server" JVM ────────────────┐
     │  outer API  →  DistributedCache  →  RaftNode ×3    │
     │  (new, thin)   (existing facade)     (loopback)     │
     └─────────────────────────────────────────────────────┘
```

- Other Java servers talk to *that one JVM*; it is the only place external writes land.
- **Zero Raft / wire / dispatch-chain changes** — the shim sits *above* the facade and
  delegates; it never re-implements commit logic (apply-then-ack untouched).
- The fastest credible path to a cross-JVM client; fully zero-dep with an in-policy transport.

## 2. Why A was rejected (the actual reasons)

1. **A central write entry point.** External writes can only reach the cluster through that
   one server JVM. If it dies, external writes are down **even when** the Raft quorum is alive
   elsewhere. B has no such failure mode: any surviving node takes a client (and, if Option C is
   ever adopted, forwards or redirects a non-leader write).
2. **B achieves the same goal without the centralization.** The stated goal is "use the cache
   cluster from another Java server." B delivers a `RemoteCacheClient` that talks to nodes
   directly over the existing wire, stays zero-dep by construction, and keeps the cluster a
   distributed service. What A additionally offered (simpler first pass, HTTP ergonomics,
   cross-language callers) is **not** required by that goal, and it costs a permanent extra
   protocol layer.
3. **One more API surface to keep honest.** A shim means a second error vocabulary, a second
   timeout, and a second async→response bridge that must track the facade's semantics
   (apply-then-ack, retryable no-leader, refusal on ambiguity) for as long as the two coexist.
   With "B is the design," those costs are pure overhead.

## 3. What A did and did not do (for completeness)

- Did **not** add any cache-client message type to the Raft wire (that is B).
- Did **not** let a write be accepted by a non-leader node (that was proposed Option C —
  deferred, and still deferred).
- Did **not** change Raft semantics, the state machine, or apply-then-ack ordering.

## 4. Cross-references

- **Chosen design** — `design/REMOTE-CLIENT-B-CLIENT-RPC.md`
- **Deferred follow-up (non-leader writes: forward vs. redirect)** —
  `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`
- **Orientation / shared ground truth** — `design/REMOTE-CLIENT.md`
- **Full original A spec** — git history, commit `7d6dbea` (file above).
