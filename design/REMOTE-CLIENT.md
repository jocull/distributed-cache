# Remote Client Access — Orientation & Options (A / B / C)

> **Status:** design/spike only. Nothing in this folder is implemented. These are hand-off
> designs for later sessions. They are deliberately *under-specified* where implementation
> is likely to discover details, and *concrete* where the answer is already fixed by the code.
> Read this file first; it links the three option specs and lays out the shared ground truth.

---

## 1. The problem (one paragraph)

Today the only way to use the cache is the in-process facade
`src/main/java/com/github/jocull/raftcache/lib/cache/DistributedCache.java`
(`put` / `delete` / `expire` / `get`). It requires live `RaftNode` Java objects in the **same
JVM** and — for writes — that the caller is co-located with the leader
(`RaftNode#submit` refuses anything but a leader). The Phase 3 network work delivered a real
socket transport, but it only moves **node↔node Raft messages** (vote, append-entries,
snapshot, `StateRequest`). There is **no wire protocol for cache client operations**, and
therefore **no way to leverage the cluster from another Java server today.** These three
options are ways to get there. A and B are the two endpoints of the solution space; C is a
small enabler that both benefit from.

---

## 2. Shared ground truth (all three options rely on this)

These are facts about the current tree, not proposals. Cite them when implementing.

- **Client facade** — `DistributedCache`. `put/delete/expire` route to the in-JVM leader and
  return `CompletableFuture<RaftLog<…>>`; `get` reads the local `PartitionMap` synchronously.
  Leader selection: `findLeaderNode()` walks the in-JVM `nodes` list, calls `getNodeState()`,
  and *refuses* if zero or more-than-one leader is reported.
- **Write durability (already built, keep it)** — leader `submit` → `AppendEntries` to
  followers → `AcknowledgeEntries` → `updateCommittedIndex()` at majority → **apply-then-
  acknowledge**: `RaftLogs#commit()` returns logs *without* acking; the node publishes
  `LogsCommitted`; `StateMachineBridge` applies to the `PartitionMap` (synchronous); **only
  then** `RaftLogs#notifyCommitted()` completes the client future. A client never sees its own
  write committed before it has taken effect on the leader's state machine.
- **Read consistency (the current state, honestly stated)** — `get` reads that node's local
  `PartitionMap`, fed from that node's own committed log as it advances. Reads never return
  uncommitted data (Raft safety floor). A **follower** read can temporarily lag a just-committed
  write (bounded by replication + apply delay) → follower reads are **eventually consistent**.
  **There is no read-index / lease mechanism** (grep confirms zero hits).
- **Wire frame** — 4-byte big-endian length + one `java.io.Serializable` payload per frame
  (`NETWORKING.md` §2.1). Closed surface; zero-dependency policy (SCOPE decision #9 / D8 —
  *no dependency may be added*, JDK `Serializable` is the codec, hand-rolled codec is the
  escape valve). **Consequence for any wire option: every type that crosses the wire must be
  `Serializable`.**
- **Transport** — `ChannelMiddleware` → `ChannelSide` (open/setReceiver/send/close).
  `SocketChannelMiddleware` = real sockets; `PassThruChannelSide` = in-memory test.
- **Dispatch (the main integration seam for wire changes)** —
  `NodeConnectionOutboundImpl#receive(Object)` is a hard-coded `instanceof` chain with an
  in-code `// TODO: Initial lazy version, not very maintainable with growing number of
  message types`. It is two-phase: **pre-introduction** (`Introduction`,
  `AnnounceClusterTopology`, `StateRequest`/`StateResponse`) vs **post-introduction**
  (everything else requires `remoteNodeId != null`). Adding a message type means touching this
  chain, plus `NodeCommunicationSender` (add a `sendX`), plus `NodeCommunicationReceiver`
  (add an `onX`), with `RaftNodeBehavior` (Leader/Follower/Candidate) providing the handlers.
- **Request/response correlation (already generic, reuse it)** — `Request` carries a `UUID
  requestId`; `Response(Request)` echoes it back. `NodeRequests#getRequestFuture(req,
  respClass)` + `completeRequest(resp)` is a generic pending-future registry keyed by
  `requestId` (currently a **5 s** timeout — in-code TODO "Is this timeout appropriate?").
  `sendStateRequest` is the working example. Any new client request can reuse this verbatim.
- **Cache operation payloads (already exist, all `Serializable`)** — `KeyDataInput`,
  `KeyDataDeleteInput`, `KeyDataExpireInput` (these are the log entries submitted). **Note:**
  the *read* payload `KeyDataOutput` is **not** `Serializable` — a wire option must fix this
  or introduce a serializable read-response type (see open questions).
- **Leader discovery (already exists, in-JVM)** — `RaftNode#getLeader()` →
  `getClusterNodeStates()` → `StateRequest` to every active peer → pick `LEADER` at max term.
  A remote client will want a networked form of this.
- **State machine / apply path** — `PartitionMap` (`getData(key) → Optional<KeyDataOutput>`),
  `StateMachineBridge` (subscribes `LogsCommitted`, replays into the map), and the
  `DistributedCache` constructor wiring a snapshot supplier onto the node the facade owns.

### Two in-code `// TODO`s these designs should respect (do not silently "fix")
- `RaftNodeImpl.java:164` — `// TODO: MEGA WEIRD!!!` — **leave intact** (standing instruction).
- `NodeConnectionOutboundImpl.java:77` — message-type-chain maintainability — *relevant* to B
  (optionally the refactor moment), but **not required** by any option.

---

## 3. The three options at a glance

| | **A — Embedded shim** | **B — Client RPC on the wire** | **C — Leader forwarding** |
|---|---|---|---|
| What | Run the cluster in one "cache server" JVM; add a thin outer API (HTTP/JSON or JDK-socket) that delegates to the in-JVM `DistributedCache` | Add cache-client message types to the existing wire protocol + a thin `RemoteCacheClient` that discovers the leader and routes ops; any node accepts clients | Let **any** node accept a client write and forward the entry to the current leader, returning the leader's commit result |
| Scope of change | **No Raft/wire changes** — a new layer above the facade | New message types + dispatch/sender/receiver seams + a client | A new node↔node forwarding pair + handlers; reuses `submit` |
| Cross-JVM client? | ✅ via the outer API (central entry point) | ✅ direct to any node | partial — is the *node's* forwarding; needs a client to be useful |
| Central point of failure? | **Yes** (the cache-server JVM is the write entry) | No (any surviving node takes clients) | Reduces it (writes work if *leader* is alive, client can hit any node) |
| Effort / risk | **Low** / low (isolated) | **High** / medium-high (touches dispatch chain) | **Low–medium** / low (self-contained) |
| Best for | Fastest cross-JVM; outer ecosystem already HTTP/gRPC | The "proper" fully-distributed client (the stated goal) | First self-contained commit; unblocks B |

**Recommended order (matches the tasking):** spike **A** first (initial design / quick path),
then **B** (the end-state design), then **C** (the enabler that makes B simpler and A more
fault-tolerant). In *implementation* terms C is the smallest and most self-contained, and B
benefits most from C — so a hand-off session may well land C before finishing B. Each spec
below states its own dependencies.

---

## 4. Cross-cutting open questions (apply to more than one option)

These are **decisions, not code** — flag them, do not silently decide them in an option spec:

1. **Read consistency policy.** Options A/B/C all inherit the "follower reads are eventually
   consistent" behavior. A linearizable-read mechanism (read-index or leases) is **orthogonal**
   to all three and can be added independently. Do **not** fold it into one option by default.
2. **Authentication / authorization.** None exists today. Any *externally reachable* surface
   (A's outer API, B's client listener) is a security decision. Open, per option.
3. **Zero-dependency policy.** Affects **A** (pick the outer transport: JDK
   `com.sun.net.httpserver` + JSON keeps it zero-dep; gRPC does not). **B** stays zero-dep by
   construction (it reuses the existing `Serializable` wire). C is zero-dep by construction.
4. **Read payload serializability.** `KeyDataOutput` is not `Serializable`. Any wire option
   serving `get` must resolve this (make it serializable, or mint a wire response type). Open.
5. **Bootstrap / discovery.** How does a *client* learn the node addresses and the current
   leader? (A: configuration of the shim endpoint; B: a bootstrap node list + a discovery
   exchange; C: inherits B's or A's.)

---

## 5. How to work in this repo (for the hand-off session)

- **Gate before every commit:** `mvn -o -B test` (currently 461 run / 0 fail / 0 error / 1
  pre-existing skip). Java floor 17.
- **House-style commits:** authored with `-c user.name="James O'Cull" -c user.email=james.ocull@gmail.com`.
- **Branching:** this is the `finish` rewrite branch. Push is a **user** action
  (`--force-with-lease`) — a hand-off session should **not** push and should confirm before
  touching push state.
- **Documentation conventions:** top-level `*.md`, uppercase, prose-heavy with in-code `// TODO`
  markers referenced by file:line. Keep this `design/` folder in the same spirit: high-level
  view + grounded seams + explicit open questions.
- **Do not "resolve" the two in-code TODOs** listed in §2 without an explicit ask.

---

## 6. Links

- Option A — `design/REMOTE-CLIENT-A-EMBEDDED-SHIM.md`
- Option B — `design/REMOTE-CLIENT-B-CLIENT-RPC.md`
- Option C — `design/REMOTE-CLIENT-C-LEADER-FORWARDING.md`
- Existing design context — `SCOPE.md` (status/decisions), `NETWORKING.md` (wire + transport),
  `SNAPSHOTS.md` (snapshot/compaction, addenda-only), `README.md`.
