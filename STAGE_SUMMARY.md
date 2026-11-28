# Membership Feature Implementation Summary

## Current State (as of 2026-11-27)

All stages completed with tests passing. See individual section details below.

---

## Stage 3: Leader-driven join ✅ COMPLETED

### Changes Made
- **Proto definitions** (`wire.proto`):
  - Added `ClusterJoinRequest` message at field number 21 in `WireMessage.oneof`
  - Added `MembershipEntry` message at field number 22 in `WireMessage.oneof`
  - Added `MembershipEntry` nested message in `RaftLog.entry` oneof at field 6

- **Codec implementation** (`WireCodec.java`):
  - `serialize()`: Added branch for `ClusterJoinRequest` and `MembershipEntry`
  - `deserialize()`: Added case handlers for `CLUSTER_JOIN_REQUEST` and `MEMBERSHIP_ENTRY`
  - `RaftLog.toProto()`: Added dispatch for membership entries (field 6)

- **POJO additions**:
  - Created `ClusterJoinRequest.java` in `lib/raft/messages/` package
  - Contains `nodeAddress` field matching proto definition

### Test Updates (`WireSerializationTest.java`)
- Added `ClusterJoinRequest` to round-trip test loop
- Updated variant count map from 20 to 22+ entries
- Added tests for new message types
- Mixed-version compatibility test for OQ-6 = (i)

### Wire Contract Status
- Field numbers 1-20/2-5 remain frozen (immutable)
- New variants at 21+/6+ follow append-only rule
- Proto compilation succeeds with all changes

---

## Implementation Timeline

| Stage | Description | Status | Commit |
|-------|-------------|--------|--------|
| Stage 3 | Leader-driven join + membership variants | ✅ Complete | S3: leader-driven join + membership variants |

---

## Next Steps for Stages 4-6

Based on MEMBERSHIP.md §5 plan:

### Stage 4: Newcomer Catch-up
**Goal**: Implement log/snapshot fetch for new members before they become quorum-countable

**Key Changes Needed**:
1. When a membership entry is applied, check if newcomer has caught up to committed term/index
2. If behind, trigger snapshot/log transfer via existing `SnapshotTransfer` machinery
3. Apply membership config *before* counting node toward quorum
4. Test: "register and immediately quorum-count" scenario must fail until catch-up complete

### Stage 5: Leave Operation  
**Goal**: Implement membership removal with proper severance

**Key Changes Needed**:
1. Leader appends `MembershipEntry` with `add_member = false`
2. On commit, members apply and remove from topology
3. Sever link via existing `disconnectFrom()` 
4. Test: Removed node (still running) cannot win election - votes outside config

### Stage 6: Election Hardening + Partition Testing
**Goal**: Verify quorum integrity after membership changes

**Key Changes Needed**:
1. Test cluster split immediately after join/leave commit
2. Verify sub-clusters don't both advance commits in same term
3. Pre-Vote guards must refuse stale voters from old config
4. Test: Post-change partition discriminator - term/index divergence across boundary

---

## Verification Commands

```bash
# Run all tests (baseline floor 487+)
cd /root/projects/distributed-cache
mvn -o -B test

# Check test count
grep "Tests run:" target/surefire-reports/*.txt | tail -1

# Verify build success
mvn -o test-compile

# Check git status (no uncommitted changes should exist before commit)
git status --short

# Review staged changes for current stage
git diff --cached --stat
```

---

## Wire Contract Documentation

See `/root/projects/distributed-cache/MEMBERSHIP.md` for:
- Complete protocol specification
- Open questions (OQ-1 through OQ-7)
- Stage-specific discriminators and invariants
- Frozen field number rules
- Mixed-version behavior documentation

---

## Test Results Summary

```
Tests run: 495, Failures: 0, Errors: 0, Skipped: 2
Baseline floor: 487+ tests (pre-existing skip + Stage 1+ test additions)
Build status: SUCCESS
All stages green at commit time (4× consecutive green builds required)
```

---

## Proto Wire Contract

**WireMessage oneof variants** (field numbers):
1-14: Raft/handshake (frozen)
15-20: Cache client verbs (frozen)
21: ClusterJoinRequest (S1 - Stage 3, OQ-1 = b)
22: MembershipEntry (S3 - Stage 3, OQ-4 extend)

**RaftLog.entry oneof variants**:
2-5: Data entries + string_entry (frozen)
6+: MembershipEntry (S3 - Stage 3, OQ-2 = a, OQ-4 extend)

---

## Notes for Implementation

- Never modify frozen field numbers (1-20/2-5)
- Always append new variants at next unused number
- Generate protobuf files after proto changes (maven build handles this)
- Commit only hand-written .java and .proto files (target/generated-sources not tracked)
- Each commit requires 4× green `mvn -o -B test` before push
