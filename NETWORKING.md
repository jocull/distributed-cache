# Node Communication Over Network — Design and Staged Plan (phase 3 / work direction of 2026-11-21)

**Status: DESIGN DELIVERED 2026-11-21; implementation begins in stages.** Per the user's direction:
*"a good place to start testing full network implementations … an actual cluster we can run, mutate,
manage, take down nodes … Then we run actual request/response against the cluster … Ideally we would
eventually have ITs for all behavior covered so far in mocked networking tests."* This document is the
scope: the wire design, the invariants, the integration-test plan that mirrors the behaviors currently
covered only by `PassThruMiddleware`, and the staged implementation order. Every code citation is
line-verified against `finish` @ `0fee1c9`.

---

## 1. Ground truth in this repo

### 1.1 The middleware contract — `lib/raft/middleware/ChannelMiddleware.java`

- `ChannelMiddleware.openChannel(RaftNode source, NodeAddress target)` returns a `ChannelSide`.
  That is the **only** entry point: everything above it (`RaftNodeImpl`, behaviors, `DistributedCache`)
  is transport-agnostic (`RaftNodeImpl.java:145-158` outbound wrap, `:161-168` inbound wrap).
- `ChannelSide` is a three-method contract: `getAddress()` (returns the **remote** node's address —
  relied on by `NodeConnectionOutboundImpl.getRemoteNodeAddress()`, `:43-45`; documented at
  `PassThruMiddleware.java:213-215` as a must-not-change contract), `send(Object)`,
  `setReceiver(Consumer<Object>)`. Messages are plain `Object`s; the closed set is exactly the types
  dispatched in `NodeConnectionOutboundImpl.receive()` (`:78-128`): `Introduction`,
  `AnnounceClusterTopology`, `StateRequest`, `StateResponse`, `VoteRequest`, `VoteResponse`,
  `PreVoteRequest`, `PreVoteResponse`, `AppendEntries`, `AcknowledgeEntries` (plus `Request`,
  `Response` as their bases).
- `PassThruMiddleware` is the **only** implementation and it is in-memory by design:
  `openChannel` looks the target up in a node registry, builds a pair, calls
  `target.acceptConnection(right).join()` before returning the left side, and delivers messages on a
  shared scheduler with a baseline 1–9 ms per hop (`PassThruMiddleware.java:154-287`). Its per-node
  `interrupt(address)`/`restore(address)` (`:142-152`) is the established fault-injection semantics:
  messages touching a down node are **dropped**, the wire never partially delivered.

### 1.2 What a real transport must move

- All 11 wire message types above. Field inspection (stage-0 grounding): every field is `String`,
  `int`, `boolean`, `NodeAddress`, `NodeIdentifier`, `NodeStates` (enum), `messages.TermIndex`,
  `List<NodeIdentifier>`, `List<AppendEntries.RaftLog>` — i.e. **the entire wire surface is closed
  and composed of value types**, with the only open-typed slot being `AppendEntries.RaftLog.entry`
  (`AppendEntries.java:51-77`).
- Log entries crossing that open slot today: `KeyDataInput` (String + `Map<String,byte[]>`),
  `KeyDataDeleteInput` (String), `KeyDataExpireInput` (String + `Duration`) — all plain POJOs; and
  plain `String` entries in `RaftNodeTest`. So the concrete set of entry classes that must be wire-
  compatible is likewise closed.
- There is **no serialization of any kind in `src/main`** (no Jackson, no protobuf, no JSON anywhere
  on the classpath in scope): this work introduces the first wire format.

### 1.3 Constraints

- **Offline Maven**: no new dependencies (local repo has only spring-boot-starter, commons-lang3,
  commons-codec, resilience4j-retry, junit, mockito). Decision (below, #D8): JDK `Serializable` is the
  format — zero new dependencies, and the wire surface is small and closed exactly as needed.
- **Java 17** (`pom.xml` release 17): no virtual threads; plain `Thread`/executors.
- `RaftNodeImpl.stop()` is terminal (`nodeExecutor.shutdownNow()`, `RaftNodeImpl.java:84-89`): a
  taken-down node is *restarted as a fresh manager*, mirroring a process restart (no persistence
  exists — see the interaction with `SNAPSHOTS.md` below).

## 2. Design

### 2.1 Wire format (frame)

```
+---------+======================================+
| length  | JDK-serialized payload stream        |
| 4 bytes | (ObjectOutputStream, length bytes)  |
| BE int  |======================================|
+---------+--------------------------------------+
```

- Length-prefixed (big-endian), one frame = one `Object`. Serialized with
  `java.io.ObjectOutputStream` / `ObjectInputStream` — the JDK's built-in binary format: every
  involved class `implements Serializable` with an explicit `serialVersionUID`.
- Rationale (recorded decision #D8 in `SCOPE.md`): no dependency is available and none may be added;
  the surface is closed (11 message types + 3 payload types + 5 value types), so the
  `Serializable`-cast cost is bounded; round-trip is byte-exact in-JVM and stable across JVMs of the
  same code. A hand-rolled codec is the escape valve if a payload type ever stops implementing
  `Serializable` or cross-version compatibility is needed — the frame format above does not change
  either way (framing is payload-neutral).
- **Invariants the format must respect** (reader discipline): the reader thread MUST always consume
  a complete frame before any drop decision (invariant N3) — dropping must never desynchronize the
  byte stream.

### 2.2 `SocketChannelMiddleware` — the second `ChannelMiddleware` impl

- **Mode: per-node server (production shape).** One middleware = one logical node (one `RaftNode`
  registered via `addNode`, exactly like `PassThruMiddleware.addNode`). The middleware owns a
  `ServerSocket` bound to `127.0.0.1` on an ephemeral port (`bind(port 0)`); the bound host:port is
  the node's wire address, exposed via `getAddress()` and also registered as its `NodeAddress` string
  so `TopologyDiscovery` can serve it. Node addresses thus become real `host:port` strings; the
  loopback IT harness assigns one middleware per node, so `openChannel(source, "127.0.0.1:PORT")`
  dials the right node's socket.
  (The registry map remains, and a single middleware *can* host several nodes by handshake-routing —
  the IT harness does not need that; it stays as the hub-mode escape valve, not the baseline.)
- **Dial + handshake.** `openChannel(source, target)`:
  1. Look up target node in registry (must exist — same contract as `PassThruMiddleware`).
  2. Dial `new Socket(host, port)`.
  3. Client writes one handshake frame first: `Handshake(clientNodeAddress, targetNodeAddress)`.
  4. Server acceptor reads the handshake, builds the server-side `ChannelSide`, calls
     `target.acceptConnection(side)` (on the target node's executor) **and joins it** — the
     `setReceiver` inside `NodeConnectionOutboundImpl`'s constructor is therefore installed before
     the client is unblocked (same guarantee `PassThruMiddleware.openChannel` gives with `.join()`).
  5. Server writes an ACK frame; client reads it and **only then** returns the client side.
     From that instant both receiver sinks exist and both reader loops run.
- **Threading.** One daemon acceptor thread per middleware; one daemon reader thread per accepted
  connection; send is per-channel synchronized (serialize → `DataOutputStream.write` under a channel
  lock, so frames from concurrent senders never interleave). Reader thread runs the loop:
  read 4-byte length (`InputStream.readNBytes`, retry until complete), read payload, deserialize,
  then deliver to the side's receiver. Delivery is sequential per channel = **FIFO wire ordering** —
  strictly stronger than `PassThru` (which reorders hops), which is fine: nothing above depends on
  reordering, and Raft's protocol is order-tolerant (term/index guards handle late arrivals).
- **Receiver buffering.** A side buffers inbound messages until `setReceiver` is called, then
  flushes in order — belt-and-braces against any setup race (the handshake order above already
  makes it impossible, the buffer keeps the side self-containedly safe).
- **Fault injection: `interrupt(address)` / `restore(address)` — PassThru parity.** Marking a
  logical node as down: that node's **outbound** sides drop `send()` (and that is all the wire
  needs on the send path); that node's **inbound** sides keep reading frames from the socket but
  discard the deserialized payload (reader discipline N3 — bytes are consumed, messages dropped;
  the stream stays framed and the far side stays in sync). `restore` stops the discarding. This is
  the same observable partition semantics as `PassThruMiddleware.interrupt` (messages touching a
  down node are lost) over a real socket, and it is **reversible**, which `RaftNodeImpl.stop()`
  is not (`nodeExecutor.shutdownNow`). The ITs' node-down scenarios use this knob first — mirroring
  exactly what `RaftNodeFlakySlowNodeTest` and the election-failure tests do — and hard stop is
  covered separately as the restart scenario (see 2.4).
- **`close()`** — close all channel sockets, close the server socket, join the acceptor and reader
  threads (all daemon, so a JVM can also always exit).

### 2.3 Serialization surface (stage A of the plan)

Add `implements Serializable` + `static final long serialVersionUID` to exactly the closed wire set:

- messages: `Request` (base, covers `Response`/`StateRequest`), `StateResponse`, `Introduction`,
  `AnnounceClusterTopology`, `VoteRequest`, `VoteResponse`, `PreVoteRequest`, `PreVoteResponse`,
  `AppendEntries` **and its inner `RaftLog`**, `AcknowledgeEntries`, `messages.TermIndex`;
- topology: `NodeAddress`, `NodeIdentifier`;
- payloads moved in `AppendEntries.RaftLog.entry` in the client API: `KeyDataInput`,
  `KeyDataDeleteInput`, `KeyDataExpireInput`.

No behavior change: these are value classes (fields are `final`, constructors already
`Objects.requireNonNull`), so adding the marker interface cannot change any existing test.

### 2.4 Node lifecycle for "take down / bring up" (IT semantics)

- **Partition (reversible)** — `middleware.interrupt(address)` / `restore(address)`: the node is
  still up but unreachable. Rejoins by Raft's normal catch-up (leader's `nextIndex` backtracking
  re-sends the missed AppendEntries entries — the same path `RaftNodeFlakySlowNodeTest` exercises).
- **Hard stop (terminal)** — `raftNode.stop()` + `middleware.close()`: the process dies. Recovery =
  **process-restart semantics**: a fresh `RaftManager` + fresh `SocketChannelMiddleware` + fresh
  node with the **same id and registry address**, re-joined to the cluster via `connectWithTopology()`.
  Its log is empty (this repo persists nothing — `SNAPSHOTS.md` §1.1), so it catches up purely by
  re-replication: the follower accepts the leader's AppendEntries from the beginning, and the apply
  path is exactly-once per (unique log entry) anyway (`StateMachineBridge` skips ≤ last-applied,
  `RaftNodeImpl` rollback/commit semantics unchanged). The data layer sees one consistent state.
  (This is the honest in-memory analogue of "node restarts and re-syncs"; durable persistence is
  Follow-up #2 and snapshots are `SNAPSHOTS.md` — the IT asserts the invariant, not the mechanism.)
- **No reconnection-within-a-channel is attempted** in this pass: a broken socket kills the channel;
  the node's `connectWithTopology`/`connectTo` paths are the re-establishment mechanisms (they
  already exist, `RaftNodeImpl.java:137-159`). Auto-reconnect policy is a recorded follow-up.

## 3. Invariants (test bar for the IT suite)

- **N1 — transport transparency.** `SocketChannelMiddleware` satisfies the `ChannelMiddleware`
  contract exactly as `PassThruMiddleware` does: `getAddress()` reports the remote node; `send`
  delivers to the peer's receiver; `openChannel` returns only once the peer has registered. Anything
  `RaftNodeImpl` does over `PassThru` must work unchanged over sockets.
- **N2 — round-trip fidelity.** Every wire type, including the open-typed entry slot
  (`KeyData*Input` payloads and String entries), deserializes to an `equals`-equal instance of the
  identical class, in order, across the real socket.
- **N3 — frame discipline.** Interrupt/restore never leaves a half-read frame on the stream: the
  reader always consumes a full frame and drops the *message*. After `restore`, both directions work
  immediately and correctly (no desync, no stuck futures on the *restored* path).
- **N4 — quorum under partition.** With 1 of 3 nodes down (either knob), the surviving pair
  continues to commit; at most one leader (per-term) is ever observable on the live pair; writes
  committed during the partition are durable on the two live nodes.
- **N5 — reconvergence.** After `restore`, a partitioned node re-joins, catches up, and the value
  for every pre-partition key is visible on **all three** local state machines (reads are served
  per-node from each node's own `PartitionMap` — the IT asserts this on all three, which PassThru's
  shared-bus harness could not).
- **N6 — restart safety.** A hard-stopped node's fresh incarnation (same id/address) re-enters the
  cluster, catches up by re-replication, and converges to the same data — no loss, no duplication
  of applied versions (`version` counter monotonic per key).

## 4. Integration-test plan (mirrors the mocked-networking behaviors)

Harness: `ClusterKit` (test scope) builds an N-node cluster over real sockets, **production-shaped**:
per node = own `SocketChannelMiddleware` (ephemeral port, 127.0.0.1) + own `RaftManager` + own
`EventBus` + own `PartitionMap` + own `DistributedCache`. So replication and apply are genuinely
per-node (distinct state machines), not the shared-bus in-process harness. Kit operations:
`awaitLeader(timeout)` — polls each node's local `getNodeState()` (no network), asserting
at-most-one-leader-per-term on **every** sample (the existing hardening rule, Follow-up #7);
`put/get/delete/expire` — via the chosen `DistributedCache` (leader routing stays in-process, as
today — network-independent client logic); `interrupt(i)/restore(i)` — partition knobs;
`hardStop(i)/restart(i)` — 2.4's terminal pair; `localViewOf(node)` for per-node read assertions.

Scenarios (each a green test before any commit):

1. **Election over sockets.** Start 3 nodes; a leader exists within the budget, one leader,
   two followers; at-most-one-leader-per-term held on every poll sample.
2. **PUT replicates + applies on all nodes.** `put` via the cache → visible in **all three**
   local maps (write-then-read per node), correct `version`.
3. **DELETE then re-PUT.** Delete visible on all three; re-PUT visible, version counter continues.
4. **EXPIRE.** Short TTL (wall-clock, generous budget): key present → gone on all nodes after expiry
   (checked-read path).
5. **Partition a follower.** Interrupt node 3 → writes keep committing on nodes 1+2 (N4) → restore →
   convergence on all three (N5).
6. **Partition the leader.** Identify leader, interrupt it → new leader within budget, writes commit
   (N4) → restore → old leader has stepped down (Follow-up #8 rule), convergence (N5).
7. **Hard-stop + restart.** Interrupt-free: `hardStop` node 3 (sockets closed — N3's irreversible
   path) → cluster commits → `restart` node 3 as a fresh incarnation with the same id/address →
   catch-up convergence (N6), `version` counters monotonic, no duplicate applies.

Budgets: election timer is 150–300 ms (`RaftNodeBehaviorFollower`'s window) — waiters use seconds,
not ms; the loopback latency cost is microseconds. The full IT suite is expected to run in well under
a minute of wall time.

## 5. Staged implementation (each stage = green suite + one commit)

Existing bar: **242 green / 0 fail / 1 skip** (the only skip is the pre-existing `@Disabled`
`proto.raft.RaftNodeTest`). Any new failure = STOP before committing.

- **Stage A — wire types Serializable + `WireSerializationTest`.** Marker interfaces on the closed
  set (§2.3) + a round-trip test for every type (build, serialize, deserialize, assert `equals`).
  No behavior change.
- **Stage B — `SocketChannelMiddleware` + unit tests.** The middleware (§2.2) with a minimal fake
  `RaftNode` (test scope): connect + echo (client→server and server→client), ordering (N2),
  interrupt/restore drop-and-resume over the real socket (N3), `close()` idempotence. No Raft yet.
- **Stage C — `ClusterKit` + ITs 1–6.** Real 3-node cluster over sockets; election, CRUD, the two
  partition scenarios with the N4/N5 assertions. The mocked-networking behaviors of items 1–3 and the
  FlakySlow test are now re-proven over the real wire.
- **Stage D — IT 7 (hard-stop/restart) + suite evidence.** N6, then flip `SCOPE.md` phase 3 to done
  (in-scope subset) with the test-evidence log updated.

## 6. Recorded, not blocking

1. **Auto-reconnect.** A dead channel stays dead; re-establishment is via `connectWithTopology`.
   Policy (backoff, who-dials-whom) is a follow-up once `disconnectFrom` (Decision #3) lands.
2. **Cross-JVM / multi-host.** The per-node-server design already dials real host:port strings, so a
   node in another JVM is addressable; not exercised here (single-JVM IT) and would need the
   serialization decision revisited for cross-version compatibility.
3. **TLS / authentication.** Out of scope; loopback IT only.
4. **`DistributedCache` leader routing over the wire.** Today it still finds the leader in-process
  (direct `RaftNode` access). Once the client is out-of-process, routing becomes "connect to any
  known node, forward to the leader" — the protocol already carries `StateResponse.identifier`, so
  this is an extension, not a redesign.
5. **Snapshot transfer** (`SNAPSHOTS.md`) is the stage-6-ish consumer of this wire: once it is
   implemented, its payload crosses the same frames — the format is payload-neutral for that.
6. **Missing/weak `equals` in the wire surface** (recorded during Stage A, 2026-11-21). The
   message classes (`TermIndex`, `AcknowledgeEntries`, the vote/pre-vote pair, `AppendEntries`,
   ...) have **no `equals`**, and `KeyDataInput.equals` compares `byte[]` values **by reference
   identity** (a latent pre-existing wart: two semantically equal inputs are never equal). Stage A
   deliberately does not touch value semantics (marker interface only), so `WireSerializationTest`
   asserts round-trip fidelity field-by-field. Adding value `equals` to the wire types (and fixing
   `KeyDataInput`) is a safe follow-up if ever needed — nothing in `src/main` relies on that
   `equals` today (verified: the bridge and `PartitionMap` compare by key, not by entry equality).

## 7. Verification

No source files change until Stage A begins. Baseline before first commit: **242 green / 0 fail /
1 skip** on `finish` @ `0fee1c9`. Each stage re-runs the full offline suite (`mvn -o -B test`),
green before commit, log cited in `SCOPE.md`'s test-evidence section (pattern:
`/tmp/opencode/mvn-net-*.log`).
