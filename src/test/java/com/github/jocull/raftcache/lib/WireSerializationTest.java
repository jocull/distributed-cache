package com.github.jocull.raftcache.lib;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.messages.AcknowledgeEntries;
import com.github.jocull.raftcache.lib.raft.messages.AnnounceClusterTopology;
import com.github.jocull.raftcache.lib.raft.messages.AppendEntries;
import com.github.jocull.raftcache.lib.raft.messages.Introduction;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.PreVoteResponse;
import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.StateRequest;
import com.github.jocull.raftcache.lib.raft.messages.StateResponse;
import com.github.jocull.raftcache.lib.raft.messages.TermIndex;
import com.github.jocull.raftcache.lib.raft.messages.VoteRequest;
import com.github.jocull.raftcache.lib.raft.messages.VoteResponse;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage A of the phase-3 networking plan (see {@code NETWORKING.md}): the wire surface is
 * {@link java.io.Serializable} (Decision #9 in {@code SCOPE.md}) and every message type,
 * value type, and cache payload must round-trip through JDK serialization to an
 * equal-value instance of the identical class -- invariant N2 (round-trip fidelity).
 *
 * <p>These tests exercise the exact serialization that the wire will use (byte stream,
 * {@link ObjectOutputStream}/{@link ObjectInputStream}) without sockets, so the encoding
 * is proven independently of {@code SocketChannelMiddleware}.
 */
class WireSerializationTest {

    private static <T> T roundTrip(T value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(value);
        }
        try (InputStream in = new ByteArrayInputStream(out.toByteArray());
             ObjectInputStream ois = new ObjectInputStream(in)) {
            @SuppressWarnings("unchecked")
            T result = (T) ois.readObject();
            return result;
        }
    }

    private static <T> List<T> roundTripList(List<T> values) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(new Object[values.size()]);
            for (T v : values) {
                oos.writeObject(v);
            }
        }
        try (InputStream in = new ByteArrayInputStream(out.toByteArray());
             ObjectInputStream ois = new ObjectInputStream(in)) {
            ois.readObject(); // consume the placeholder array
            List<T> result = new java.util.ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                @SuppressWarnings("unchecked")
                T next = (T) ois.readObject();
                result.add(next);
            }
            return result;
        }
    }

    private static void assertKeyDataInput(String key, Map<String, byte[]> data, KeyDataInput actual) {
        assertEquals(key, actual.getKey());
        assertEquals(data.keySet(), actual.getData().keySet());
        for (Map.Entry<String, byte[]> e : data.entrySet()) {
            assertTrue(Arrays.equals(e.getValue(), actual.getData().get(e.getKey())),
                    "value for " + e.getKey() + " must match byte-for-byte");
        }
    }

    private static void assertTermIndexFields(int term, long index, TermIndex ti) {
        assertEquals(term, ti.getTerm());
        assertEquals(index, ti.getIndex());
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestAndResponseRoundTripPreservingTheUuid() throws Exception {
        Request request = new StateRequest();
        Request req = roundTrip(request);
        assertEquals(request.getRequestId(), req.getRequestId());
        assertEquals(StateRequest.class, req.getClass());

        StateResponse response = new StateResponse(request, "node-1",
                new NodeAddress("127.0.0.1:7001"), NodeStates.LEADER, 7,
                new TermIndex(7, 42), new TermIndex(7, 40));
        StateResponse resp = roundTrip(response);
        assertEquals(request.getRequestId(), resp.getRequestId());
        assertEquals("node-1", resp.getIdentifier().getId());
        assertEquals("127.0.0.1:7001", resp.getIdentifier().getNodeAddress().getAddress());
        assertEquals(NodeStates.LEADER, resp.getState());
        assertEquals(7, resp.getTerm());
        assertEquals(42, resp.getCurrentTermIndex().getIndex());
        assertEquals(40, resp.getCommittedTermIndex().getIndex());
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
    void valueTypesRoundTrip() throws Exception {
        NodeAddress address = new NodeAddress("host.example:");
        assertEquals(address, roundTrip(address));

        com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier mid =
                new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier("id-9", new NodeAddress("10.0.0.1:8080"));
        com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier midBack = roundTrip(mid);
        assertEquals("id-9", midBack.getId());
        assertEquals("10.0.0.1:8080", midBack.getNodeAddress().getAddress());

        com.github.jocull.raftcache.lib.topology.NodeIdentifier tid =
                new com.github.jocull.raftcache.lib.topology.NodeIdentifier("id-10", new NodeAddress("10.0.0.2:1234"));
        com.github.jocull.raftcache.lib.topology.NodeIdentifier tidBack = roundTrip(tid);
        assertEquals("id-10", tidBack.getId());
        assertEquals("10.0.0.2:1234", tidBack.getNodeAddress().getAddress());

        TermIndex index = new TermIndex(1, Long.MAX_VALUE);
        TermIndex indexBack = roundTrip(index);
        assertEquals(1, indexBack.getTerm());
        assertEquals(Long.MAX_VALUE, indexBack.getIndex());
    }

    @Test
    void cachePayloadsRoundTrip() throws Exception {
        Map<String, byte[]> data = Map.of(
                "field-a", new byte[]{1, 2, 3},
                "field-b", new byte[]{(byte) 0xFF, 0});
        KeyDataInput put = new KeyDataInput("orders:1", data);
        assertKeyDataInput("orders:1", data, roundTrip(put));
        // Note (Stage A): KeyDataInput#equals does byte[] values by identity, so two
        // semantically equal inputs (different byte[] instances) are NOT equal -- a latent
        // pre-existing wart, recorded in NETWORKING.md, out of scope to fix in this stage.

        KeyDataDeleteInput delete = new KeyDataDeleteInput("orders:1");
        assertEquals(delete, roundTrip(delete));

        KeyDataExpireInput expire = new KeyDataExpireInput("orders:1", Duration.ofSeconds(45));
        assertEquals(expire, roundTrip(expire));
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
    void emptyAppendEntriesHeartbeatRoundTrip() throws Exception {
        AppendEntries ae = new AppendEntries(5, new TermIndex(4, 100), new TermIndex(5, 100), List.of());
        AppendEntries back = roundTrip(ae);
        assertEquals(5, back.getTerm());
        assertTrue(back.getEntries().isEmpty());
    }

    @Test
    void serializedStreamKeepsMessageOrder() throws Exception {
        List<Object> wire = List.of(
                new Introduction("n1", new NodeAddress("127.0.0.1:1"), new TermIndex(1, 1)),
                new VoteRequest(2, "n2", new TermIndex(2, 5)),
                new AppendEntries.RaftLog(new TermIndex(2, 6), new KeyDataInput("z", Map.of("f", new byte[]{1}))),
                new AcknowledgeEntries(2, true, new TermIndex(2, 6)),
                new KeyDataExpireInput("t", Duration.ofMinutes(3)));

        List<Object> back = roundTripList(wire);
        assertEquals(wire.size(), back.size());
        assertSame(wire.get(0).getClass(), back.get(0).getClass());
        assertEquals("n1", ((Introduction) back.get(0)).getId());
        assertEquals("n2", ((VoteRequest) back.get(1)).getCandidateId());
        assertTermIndexFields(2, 6, ((AppendEntries.RaftLog) back.get(2)).getTermIndex());
        AcknowledgeEntries ack = (AcknowledgeEntries) back.get(3);
        assertEquals(2, ack.getTerm());
        assertTrue(ack.isSuccess());
        assertTermIndexFields(2, 6, ack.getCurrentTermIndex());
        assertEquals(wire.get(4), back.get(4));
    }

    @Test
    void everyWireTypeIsSerializable() {
        // The closed wire set from NETWORKING.md section 2.3 must each implement Serializable.
        for (Class<?> type : List.of(
                Request.class, StateRequest.class, StateResponse.class,
                Introduction.class, AnnounceClusterTopology.class,
                VoteRequest.class, VoteResponse.class,
                PreVoteRequest.class, PreVoteResponse.class,
                AcknowledgeEntries.class, AppendEntries.class, AppendEntries.RaftLog.class,
                com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier.class,
                TermIndex.class,
                NodeAddress.class,
                com.github.jocull.raftcache.lib.topology.NodeIdentifier.class,
                KeyDataInput.class, KeyDataDeleteInput.class, KeyDataExpireInput.class)) {
            assertTrue(Serializable.class.isAssignableFrom(type),
                    type.getName() + " must be Serializable for the wire");
        }
        // UUID (the Request envelope id) is the only other object on the wire and is Serializable.
        assertTrue(Serializable.class.isAssignableFrom(UUID.class));
    }
}
