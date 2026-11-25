package com.github.jocull.raftcache.lib;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshot;
import com.github.jocull.raftcache.lib.raft.messages.InstallSnapshotResponse;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.TermIndex;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.WireCodec;
import com.github.jocull.raftcache.lib.raft.messages.proto.WireMessage;
import com.github.jocull.raftcache.lib.raft.middleware.Acknowledgement;
import com.github.jocull.raftcache.lib.raft.middleware.Handshake;
import com.github.jocull.raftcache.lib.data.DataVersion;
import com.github.jocull.raftcache.lib.data.Snapshot;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage A (phase 3, see {@code NETWORKING.md}) re-pinned on the Task 1 wire codec:
 * every wire surface must round-trip through {@link WireCodec} — POJO in, the
 * {@link WireMessage} envelope on the wire, an equal-value instance of the
 * identical class out — invariant N2 (round-trip fidelity).
 *
 * <p>The codec (not a socket) is exercised directly, so the byte mapping is proven
 * independently of {@code SocketChannelMiddleware}: the middleware adds the 4-byte
 * length prefix and the frame cap, and its own tests drive that seam over real
 * sockets.
 */
class WireSerializationTest {

    private static <T> T roundTrip(T wireMessage) throws Exception {
        return (T) WireCodec.deserialize(WireCodec.serialize(wireMessage));
    }

    private static void assertKeyDataInput(String key, Map<String, byte[]> data, KeyDataInput actual) {
        assertEquals(key, actual.getKey());
        assertEquals(data.keySet(), actual.getData().keySet());
        for (Map.Entry<String, byte[]> e : data.entrySet()) {
            assertTrue(java.util.Arrays.equals(e.getValue(), actual.getData().get(e.getKey())),
                    "value for " + e.getKey() + " must match byte-for-byte");
        }
    }

    private static void assertTermIndexFields(int term, long index, TermIndex ti) {
        assertEquals(term, ti.getTerm());
        assertEquals(index, ti.getIndex());
    }

    @Test
    void requestAndResponseRoundTripPreservingTheUuid() throws Exception {
        StateRequest request = new StateRequest();
        Request req = roundTrip(request);
        assertEquals(request.getRequestId(), req.getRequestId(), "the exact UUID crosss the wire");
        assertEquals(StateRequest.class, req.getClass());

        // Long.MAX_VALUE index: the TermIndex fields are int64 on the wire and must not overflow.
        StateResponse response = new StateResponse(request, "node-1",
                new NodeAddress("127.0.0.1:7001"), NodeStates.LEADER, 7,
                new TermIndex(7, 42), new TermIndex(7, Long.MAX_VALUE));
        StateResponse resp = roundTrip(response);
        assertEquals(request.getRequestId(), resp.getRequestId());
        assertEquals("node-1", resp.getIdentifier().getId());
        assertEquals("127.0.0.1:7001", resp.getIdentifier().getNodeAddress().getAddress());
        assertEquals(NodeStates.LEADER, resp.getState());
        assertEquals(7, resp.getTerm());
        assertEquals(42, resp.getCurrentTermIndex().getIndex());
        assertEquals(Long.MAX_VALUE, resp.getCommittedTermIndex().getIndex());
        assertEquals(7, resp.getCurrentTermIndex().getTerm());
    }

    @Test
    void introductionRoundTrip() throws Exception {
        NodeAddress address = new NodeAddress("127.0.0.1:7002");
        Introduction introduction = new Introduction("node-2", address, new TermIndex(3, 11));
        Introduction back = roundTrip(introduction);
        assertEquals("node-2", back.getId());
        assertEquals(address, back.getNodeAddress());
        assertEquals(3, back.getLastReceivedTermIndex().getTerm());
        assertEquals(11, back.getLastReceivedTermIndex().getIndex());
    }

    @Test
    void announceClusterTopologyRoundTripPreservesOrderAndContent() throws Exception {
        AnnounceClusterTopology announce = new AnnounceClusterTopology(List.of(
                new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("a", new NodeAddress("127.0.0.1:1")),
                new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("b", new NodeAddress("127.0.0.1:2")),
                new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("c", new NodeAddress("127.0.0.1:3"))));
        AnnounceClusterTopology back = roundTrip(announce);
        assertEquals(3, back.getNodeIdentifierStates().size());
        List<com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier> ids = back.getNodeIdentifierStates();
        assertEquals("a", ids.get(0).getId());
        assertEquals("127.0.0.1:1", ids.get(0).getNodeAddress().getAddress());
        assertEquals("c", ids.get(2).getId());
        assertEquals("127.0.0.1:3", ids.get(2).getNodeAddress().getAddress());
    }

    @Test
    void voteMessagesRoundTrip() throws Exception {
        TermIndex logIndex = new TermIndex(5, 99);
        VoteRequest vote = roundTrip(new VoteRequest(9, "candidate-x", logIndex));
        assertEquals(9, vote.getTerm());
        assertEquals("candidate-x", vote.getCandidateId());
        assertEquals(5, vote.getLastLogTermIndex().getTerm());
        assertEquals(99, vote.getLastLogTermIndex().getIndex());

        VoteResponse voteResp = roundTrip(new VoteResponse(9, logIndex, true));
        assertEquals(9, voteResp.getTerm());
        assertTermIndexFields(5, 99, voteResp.getCommittedTermIndex());
        assertTrue(voteResp.isVoteGranted());
    }

    @Test
    void preVoteMessagesRoundTrip() throws Exception {
        PreVoteRequest pre = roundTrip(new PreVoteRequest(4, "candidate-y", new TermIndex(4, 7)));
        assertEquals(4, pre.getTerm());
        assertEquals("candidate-y", pre.getCandidateId());
        assertEquals(7, pre.getLastLogTermIndex().getIndex());

        PreVoteResponse preResp = roundTrip(new PreVoteResponse(4, false));
        assertEquals(4, preResp.getTerm());
        assertFalse(preResp.isPreVoteGranted());
    }

    @Test
    void acknowledgeEntriesRoundTrip() throws Exception {
        AcknowledgeEntries ack = roundTrip(new AcknowledgeEntries(6, true, new TermIndex(6, 21)));
        assertEquals(6, ack.getTerm());
        assertTrue(ack.isSuccess());
        assertEquals(21, ack.getCurrentTermIndex().getIndex());

        AcknowledgeEntries deny = roundTrip(new AcknowledgeEntries(6, false, new TermIndex(6, 5)));
        assertEquals(6, deny.getTerm());
        assertFalse(deny.isSuccess());
    }

    @Test
    void handshakeAndAcknowledgementRoundTrip() throws Exception {
        Handshake handshake = new Handshake(new NodeAddress("client.example:9000"), new NodeAddress("127.0.0.1:7001"));
        Handshake back = roundTrip(handshake);
        assertEquals("client.example:9000", back.getClient().getAddress());
        assertEquals("127.0.0.1:7001", back.getTarget().getAddress());

        Acknowledgement ack = roundTrip(new Acknowledgement());
        assertEquals(Acknowledgement.class, ack.getClass());
    }

    @Test
    void installSnapshotRoundTripWithSerializedChunkPayload() throws Exception {
        // Stage 6a: the wire payload is a byte[] chunk -- the leader's in-band serialized
        // state-machine capture. N2 fidelity applies to BOTH layers: the message round-trips
        // the chunk byte-for-byte, and the chunk decodes to the captured object.
        Snapshot snapshot = new Snapshot(6, 21, Map.of(
                "alpha", new DataVersion(Map.of("f1", new byte[]{1, 2}), 7, 1_700_000_000_000L),
                "beta", new DataVersion(Map.of("f1", new byte[]{9}), 3)));
        byte[] serialized = serializedBytes(snapshot);

        // A whole-capture single chunk (the common case for small state machines):
        // offset=0, done=true.
        InstallSnapshot install = roundTrip(new InstallSnapshot(6, "leader-1", 21, 6, 0L, serialized, true));
        assertEquals(6, install.getTerm());
        assertEquals("leader-1", install.getLeaderId());
        assertEquals(21, install.getLastIncludedIndex());
        assertEquals(6, install.getLastIncludedTerm());
        assertEquals(0, install.getOffset());
        assertTrue(install.isDone());
        assertTrue(install.getPayload() instanceof byte[], "stage 6a: the wire payload is a byte[] chunk");
        assertArrayEquals(serialized, (byte[]) install.getPayload(), "the chunk survives the wire byte-for-byte");

        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(serialized))) {
            Snapshot decoded = (Snapshot) ois.readObject();
            assertEquals(6, decoded.term());
            assertEquals(21, decoded.index());
            assertEquals(snapshot.entries().keySet(), decoded.entries().keySet());
            for (String key : snapshot.entries().keySet()) {
                DataVersion original = snapshot.entries().get(key);
                DataVersion deserialized = decoded.entries().get(key);
                assertEquals(original.getVersion(), deserialized.getVersion());
                assertEquals(original.getExpiresAtEpochMillis(), deserialized.getExpiresAtEpochMillis());
                assertEquals(original.getData().keySet(), deserialized.getData().keySet());
                for (String field : original.getData().keySet()) {
                    assertTrue(java.util.Arrays.equals(original.getData().get(field), deserialized.getData().get(field)),
                            "entry for " + key + "/" + field + " must match byte-for-byte");
                }
            }
        }

        // A mid-transfer chunk (done=false) must round-trip identically: same slice,
        // same non-zero offset.
        byte[] half = java.util.Arrays.copyOf(serialized, serialized.length / 2);
        InstallSnapshot mid = roundTrip(new InstallSnapshot(6, "leader-1", 21, 6, half.length, half, false));
        assertEquals(half.length, mid.getOffset());
        assertFalse(mid.isDone());
        assertArrayEquals(half, (byte[]) mid.getPayload());

        // The response carries the follower's next-expected-byte cursor (stage 6a). Its
        // 2-argument legacy form round-trips with a zero cursor.
        InstallSnapshotResponse done = roundTrip(new InstallSnapshotResponse(6, 21, (long) serialized.length));
        assertEquals(6, done.getTerm());
        assertEquals(21, done.getIndex());
        assertEquals((long) serialized.length, done.getOffset());

        InstallSnapshotResponse legacy = roundTrip(new InstallSnapshotResponse(6, 21));
        assertEquals(6, legacy.getTerm());
        assertEquals(21, legacy.getIndex());
        assertEquals(0L, legacy.getOffset(), "a legacy 2-arg response reports a zero cursor");
    }

    private static byte[] serializedBytes(Object value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(out)) {
            oos.writeObject(value);
        }
        return out.toByteArray();
    }

    @Test
    void cachePayloadsRoundTripAsRaftLogEntries() throws Exception {
        // The KeyData* payloads are NOT envelope variants (wire.proto): they ride as
        // RaftLog oneof entries, so pin them the way the wire carries them.
        Map<String, byte[]> data = Map.of(
                "field-a", new byte[]{1, 2, 3},
                "field-b", new byte[]{(byte) 0xFF, 0});
        KeyDataInput put = new KeyDataInput("orders:1", data);
        KeyDataInput putBack = (KeyDataInput) roundTripEntry(new AppendEntries.RaftLog(new TermIndex(1, 1), put));
        assertKeyDataInput("orders:1", data, putBack);
        // Note (Stage A): KeyDataInput#equals does byte[] values by identity, so two
        // semantically equal inputs (different byte[] instances) are NOT equal -- a latent
        // pre-existing wart, recorded in NETWORKING.md, out of scope to fix in this stage.

        KeyDataDeleteInput delete = new KeyDataDeleteInput("orders:1");
        assertEquals(delete, roundTripEntry(new AppendEntries.RaftLog(new TermIndex(1, 2), delete)));

        KeyDataExpireInput expire = new KeyDataExpireInput("orders:1", Duration.ofSeconds(45));
        assertEquals(expire, roundTripEntry(new AppendEntries.RaftLog(new TermIndex(1, 3), expire)));
    }

    private static Object roundTripEntry(AppendEntries.RaftLog entry) throws Exception {
        AppendEntries ae = roundTrip(new AppendEntries(1, new TermIndex(1, 0), new TermIndex(1, 0), List.of(entry)));
        assertEquals(1, ae.getEntries().size());
        return ae.getEntries().get(0).getEntry();
    }

    @Test
    void keyDataOutputRoundTripsAsTheReadPayload() throws Exception {
        // B §3.2: a read reply carries a KeyDataOutput (key + data + version). It is not
        // an envelope frame — the codec exposes it as a named decode (WireCodec#decodeKeyDataOutput)
        // because the transport knows from the call site it is the reply to a read.
        Map<String, byte[]> data = Map.of("f1", new byte[]{1, 2}, "f2", new byte[]{(byte) 0xFF});
        KeyDataOutput output = new KeyDataOutput("orders:1", data, 42L);
        byte[] bytes = WireCodec.serialize(output);
        KeyDataOutput back = WireCodec.decodeKeyDataOutput(bytes);
        assertEquals("orders:1", back.getKey());
        assertEquals(42L, back.getVersion());
        assertEquals(data.keySet(), back.getData().keySet());
        for (Map.Entry<String, byte[]> e : data.entrySet()) {
            assertArrayEquals(e.getValue(), back.getData().get(e.getKey()),
                    "value for " + e.getKey() + " must match byte-for-byte");
        }
    }

    @Test
    void appendEntriesRoundTripPreservesEveryEntrySlot() throws Exception {
        KeyDataInput put = new KeyDataInput("k", Map.of("f", new byte[]{9}));
        KeyDataDeleteInput delete = new KeyDataDeleteInput("k");
        KeyDataExpireInput expire = new KeyDataExpireInput("k", Duration.ofMillis(1234));
        String heartbeat = "plain string entry";

        AppendEntries ae = new AppendEntries(3, new TermIndex(2, 8), new TermIndex(3, 12),
                List.of(
                        new AppendEntries.RaftLog(new TermIndex(3, 9), put),
                        new AppendEntries.RaftLog(new TermIndex(3, 10), delete),
                        new AppendEntries.RaftLog(new TermIndex(3, 11), expire),
                        new AppendEntries.RaftLog(new TermIndex(3, 12), heartbeat)));
        AppendEntries back = roundTrip(ae);

        assertEquals(3, back.getTerm());
        assertEquals(2, back.getPreviousLogTermIndex().getTerm());
        assertEquals(8, back.getPreviousLogTermIndex().getIndex());
        assertEquals(12, back.getLeaderCommitTermIndex().getIndex());
        assertEquals(4, back.getEntries().size());

        assertEquals(9, back.getEntries().get(0).getTermIndex().getIndex());
        assertEquals(KeyDataInput.class, back.getEntries().get(0).getEntry().getClass());
        assertKeyDataInput("k", Map.of("f", new byte[]{9}), (KeyDataInput) back.getEntries().get(0).getEntry());

        assertEquals(delete, back.getEntries().get(1).getEntry());
        assertEquals(KeyDataDeleteInput.class, back.getEntries().get(1).getEntry().getClass());

        assertEquals(expire, back.getEntries().get(2).getEntry());
        assertEquals(KeyDataExpireInput.class, back.getEntries().get(2).getEntry().getClass());

        assertEquals(heartbeat, back.getEntries().get(3).getEntry());
        assertEquals(String.class, back.getEntries().get(3).getEntry().getClass());
    }

    @Test
    void appendEntriesRejectsAnEntryTypeTheStateMachineDoesNotApply() {
        // The RaftLog oneof is the closed set the state machine applies
        // (KeyDataInput / KeyDataDeleteInput / KeyDataExpireInput / String). A POJO
        // that must ride the log must fail loudly here, not silently on the wire.
        AppendEntries bad = new AppendEntries(3, new TermIndex(2, 8), new TermIndex(3, 12),
                List.of(new AppendEntries.RaftLog(new TermIndex(3, 9), new java.util.ArrayList<>())));
        assertThrows(IllegalArgumentException.class, () -> WireCodec.serialize(bad));
    }

    @Test
    void emptyAppendEntriesHeartbeatRoundTrip() throws Exception {
        AppendEntries ae = new AppendEntries(5, new TermIndex(4, 100), new TermIndex(5, 100), List.of());
        AppendEntries back = roundTrip(ae);
        assertEquals(5, back.getTerm());
        assertTrue(back.getEntries().isEmpty());
    }

    @Test
    void envelopeKeepsMessageOrderPerFrame() throws Exception {
        // Each frame is one WireMessage envelope; a stream of frames keeps both order and
        // per-frame class identity (the receiver's instanceof dispatch chain).
        List<Object> wire = List.of(
                new Introduction("n1", new NodeAddress("127.0.0.1:1"), new TermIndex(1, 1)),
                new VoteRequest(2, "n2", new TermIndex(2, 5)),
                new Handshake(new NodeAddress("c:1"), new NodeAddress("t:1")),
                new AcknowledgeEntries(2, true, new TermIndex(2, 6)),
                new Acknowledgement());

        List<Object> back = new java.util.ArrayList<>(wire.size());
        for (Object message : wire) {
            back.add(WireCodec.deserialize(WireCodec.serialize(message)));
        }
        assertEquals(wire.size(), back.size());
        assertSame(wire.get(0).getClass(), back.get(0).getClass());
        assertEquals("n1", ((Introduction) back.get(0)).getId());
        assertEquals("n2", ((VoteRequest) back.get(1)).getCandidateId());
        assertEquals("t:1", ((Handshake) back.get(2)).getTarget().getAddress());
        AcknowledgeEntries ack = (AcknowledgeEntries) back.get(3);
        assertEquals(2, ack.getTerm());
        assertTrue(ack.isSuccess());
        assertTermIndexFields(2, 6, ack.getCurrentTermIndex());
        assertSame(wire.get(4).getClass(), back.get(4).getClass());
    }

    @Test
    void unknownFieldsFromANewerSenderAreTolerated() throws Exception {
        // Protocol Buffers is versioned by design: a field a receiver does not know
        // (added by a newer sender, or a different implementation of the IDL) must not
        // break the decode. The receiver still recovers every field it does know.
        WireMessage base = WireMessage.newBuilder()
                .setIntroduction(com.github.jocull.raftcache.lib.raft.messages.proto.Introduction.newBuilder()
                        .setId("n1")
                        .build())
                .build();
        // Unknown extension field 900 (varint) + the real envelope bytes. Tag = 900<<3|0.
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(frame);
        output.writeRawVarint64(7200L);
        output.writeRawVarint64(7L);
        frame.write(base.toByteArray());

        Object back = WireCodec.deserialize(frame.toByteArray());
        assertTrue(back instanceof Introduction, "unknown fields must not disturb the envelope");
        assertEquals("n1", ((Introduction) back).getId());
    }

    @Test
    void closedWireSetMatchesTheEnvelopeVariants() throws Exception {
        // (1) The envelope is a closed set of exactly 14 variants, field numbers 1..14 —
        // this pin freezes the numbering a cross-language sender (the Task 2 client)
        // would rely on.
        WireMessage.MessageCase[] variants = WireMessage.MessageCase.values();
        assertEquals(15, variants.length, "MESSAGE_NOT_SET + 14 frozen variants");
        Map<WireMessage.MessageCase, Integer> expected = new java.util.EnumMap<>(WireMessage.MessageCase.class);
        expected.put(WireMessage.MessageCase.ACKNOWLEDGE_ENTRIES, 1);
        expected.put(WireMessage.MessageCase.ANNOUNCE_CLUSTER_TOPOLOGY, 2);
        expected.put(WireMessage.MessageCase.APPEND_ENTRIES, 3);
        expected.put(WireMessage.MessageCase.INSTALL_SNAPSHOT, 4);
        expected.put(WireMessage.MessageCase.INSTALL_SNAPSHOT_RESPONSE, 5);
        expected.put(WireMessage.MessageCase.INTRODUCTION, 6);
        expected.put(WireMessage.MessageCase.PRE_VOTE_REQUEST, 7);
        expected.put(WireMessage.MessageCase.PRE_VOTE_RESPONSE, 8);
        expected.put(WireMessage.MessageCase.STATE_REQUEST, 9);
        expected.put(WireMessage.MessageCase.STATE_RESPONSE, 10);
        expected.put(WireMessage.MessageCase.VOTE_REQUEST, 11);
        expected.put(WireMessage.MessageCase.VOTE_RESPONSE, 12);
        expected.put(WireMessage.MessageCase.HANDSHAKE, 13);
        expected.put(WireMessage.MessageCase.ACKNOWLEDGEMENT, 14);
        assertEquals(14, expected.size());
        for (WireMessage.MessageCase variant : variants) {
            if (variant == WireMessage.MessageCase.MESSAGE_NOT_SET) {
                continue;
            }
            assertEquals((int) expected.get(variant), variant.getNumber(),
                    "frozen field number for " + variant.name() + " drifted");
        }

        // (2) Every one of the 14 POJO frame types round-trips to its exact class through
        // the codec (the receiver's instanceof dispatch chain).
        for (Object frame : List.of(
                new Introduction("n1", new NodeAddress("127.0.0.1:1"), new TermIndex(1, 1)),
                new AnnounceClusterTopology(List.of(
                        new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("a", new NodeAddress("127.0.0.1:1")))),
                new StateRequest(),
                new StateResponse(new StateRequest(), "n1", new NodeAddress("127.0.0.1:1"),
                        NodeStates.FOLLOWER, 1, new TermIndex(1, 1), new TermIndex(1, 1)),
                new VoteRequest(1, "n1", new TermIndex(1, 1)),
                new VoteResponse(1, new TermIndex(1, 1), false),
                new PreVoteRequest(1, "n1", new TermIndex(1, 1)),
                new PreVoteResponse(1, false),
                new AcknowledgeEntries(1, true, new TermIndex(1, 1)),
                new AppendEntries(1, new TermIndex(1, 1), new TermIndex(1, 1), List.of()),
                new InstallSnapshot(1, "leader", 1, 1, 0L, new byte[]{1, 2, 3}, true),
                new InstallSnapshotResponse(1, 1, 0L),
                new Handshake(new NodeAddress("c:1"), new NodeAddress("t:1")),
                new Acknowledgement())) {
            Object back = WireCodec.deserialize(WireCodec.serialize(frame));
            assertEquals(frame.getClass(), back.getClass(),
                    frame.getClass().getName() + " must round-trip to its exact class");
            // The envelope's type tag must be a valid frozen variant (no drift outside 1..14).
            int number = WireMessage.parseFrom(WireCodec.serialize(frame)).getMessageCase().getNumber();
            assertTrue(number >= 1 && number <= 14,
                    frame.getClass().getName() + " must carry a valid envelope tag, got " + number);
        }

        // (3) Anything OUTSIDE the closed set is rejected with a clear error — including the
        // value types and payloads that must ride INSIDE their frames (not as envelope variants).
        assertThrows(IllegalArgumentException.class, () -> WireCodec.serialize(new TermIndex(1, 1)),
                "TermIndex is a value type, not a frame");
        assertThrows(IllegalArgumentException.class, () -> WireCodec.serialize(new NodeAddress("127.0.0.1:1")),
                "NodeAddress is a value type, not a frame");
        assertThrows(IllegalArgumentException.class,
                () -> WireCodec.serialize(new KeyDataInput("k", Map.of("f", new byte[]{1}))),
                "KeyDataInput rides inside an AppendEntries entry, not as an envelope variant");
        assertThrows(IllegalArgumentException.class, () -> WireCodec.serialize(new Snapshot(1, 1, Map.of())),
                "a state-machine type never rides a raw wire frame");
    }
}