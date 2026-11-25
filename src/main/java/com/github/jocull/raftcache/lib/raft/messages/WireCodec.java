package com.github.jocull.raftcache.lib.raft.messages;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.communication.KeyDataOutput;
import com.github.jocull.raftcache.lib.raft.NodeStates;
import com.github.jocull.raftcache.lib.raft.middleware.Acknowledgement;
import com.github.jocull.raftcache.lib.raft.middleware.Handshake;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.raft.messages.proto.WireMessage;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The wire codec (Task 1, Step B): the single boundary between the JVM message
 * types of {@code lib/raft/messages} (+ the two channel frames) and the Protocol
 * Buffers contract in {@code src/main/proto/wire.proto}. Everything the node
 * dispatches on crosses this class — and only this class.
 *
 * <h2>Frame contract</h2>
 * One {@link WireMessage} per frame (the envelope {@code oneof} in
 * {@code wire.proto}). The 4-byte big-endian length prefix, the 64 MB frame cap,
 * and the framing itself stay in {@code SocketChannelMiddleware}; this codec maps
 * the bytes inside one frame. The envelope is the wire-level type tag: a receiver
 * picks the message decoder from it WITHOUT loading any JVM class, which is the
 * cross-language property the remote client (Task 2) needs.
 *
 * <h2>Closed set</h2>
 * Exactly 20 types ride the wire as frames:
 * {@link Introduction}, {@link AnnounceClusterTopology}, {@link StateRequest},
 * {@link StateResponse}, {@link VoteRequest}, {@link VoteResponse},
 * {@link PreVoteRequest}, {@link PreVoteResponse}, {@link AppendEntries},
 * {@link AcknowledgeEntries}, {@link InstallSnapshot}, {@link InstallSnapshotResponse},
 * {@link Handshake}, {@link Acknowledgement} — and, since client B step B1
 * (2026-11-25, B §3.1), the six client cache verbs: {@link CachePutRequest},
 * {@link CacheDeleteRequest}, {@link CacheExpireRequest}, {@link CacheReadRequest},
 * {@link CacheWriteResponse}, {@link CacheReadResponse}.
 * Value types ({@link TermIndex}, both {@code NodeIdentifier}s, {@link NodeAddress})
 * and the {@code KeyData*} payloads are NOT frame types — they travel as nested
 * fields of the messages that carry them ({@code RaftLog.oneof entry} and
 * {@code InstallSnapshot.payload} bytes). Anything else handed to
 * {@link #serialize(Object)} is rejected with a clear error, so a POJO that must
 * cross the wire gets a loud failure here rather than a silent wire miss.
 *
 * <h2>Fidelity notes</h2>
 * <ul>
 *   <li>A {@link StateRequest} round-trips its exact {@link UUID}: the receiver
 *       correlates the {@link StateResponse} by request id
 *       ({@code NodeRequests#completeRequest}), so the id is reconstructed via the
 *       package-private {@code Request(UUID)} factories, not re-drawn.</li>
 *   <li>{@link AppendEntries.RaftLog#getEntry()} is typed {@code Object}; the codec
 *       dispatches on the four entry types the state machine applies
 *       ({@link KeyDataInput} / {@link KeyDataDeleteInput} / {@link KeyDataExpireInput}
 *       / {@link String}) — the same closed set as the proto {@code RaftLog} oneof —
 *       and preserves the nested-class identity of the entry.</li>
 *   <li>{@link InstallSnapshot#getPayload()} must be a {@code byte[]} chunk on the
 *       way out (it is the leader's serialized state-machine capture) and comes
 *       back byte-for-byte.</li>
 *   <li>{@code map<string, bytes>}: the POJO side copies values ({@code Map.copyOf}
 *       / immutable lists), so a round-tripped {@code Map} is a fresh immutable
 *       instance with equal contents, byte-for-byte values — not the same arrays.</li>
 * </ul>
 *
 * <p>This class lives in {@code lib.raft.messages} so it can call the
 * package-private wire-reconstruction factories on {@code Request}/{@code StateRequest}.
 */
public final class WireCodec {
    private static final int FRAME_VARIANTS = 20;

    private WireCodec() {
    }

    /**
     * Encode one wire frame (one of the 20 frame types) to its protocol-buffers bytes.
     *
     * @param message a wire message — one of the closed set above, or one of the
     *                {@code KeyData*} payloads (for the read response, B §3.2)
     * @return the frame bytes (the length prefix is added by the transport)
     * @throws IllegalArgumentException if the type is not in the closed set — the
     *                                  message names the unsupported type
     */
    public static byte[] serialize(Object message) {
        if (message instanceof Introduction) {
            return WireMessage.newBuilder().setIntroduction(toProto((Introduction) message)).build().toByteArray();
        }
        if (message instanceof AnnounceClusterTopology) {
            return WireMessage.newBuilder().setAnnounceClusterTopology(toProto((AnnounceClusterTopology) message)).build().toByteArray();
        }
        if (message instanceof StateRequest) {
            return WireMessage.newBuilder().setStateRequest(toProto((StateRequest) message)).build().toByteArray();
        }
        if (message instanceof StateResponse) {
            return WireMessage.newBuilder().setStateResponse(toProto((StateResponse) message)).build().toByteArray();
        }
        if (message instanceof VoteRequest) {
            return WireMessage.newBuilder().setVoteRequest(toProto((VoteRequest) message)).build().toByteArray();
        }
        if (message instanceof VoteResponse) {
            return WireMessage.newBuilder().setVoteResponse(toProto((VoteResponse) message)).build().toByteArray();
        }
        if (message instanceof PreVoteRequest) {
            return WireMessage.newBuilder().setPreVoteRequest(toProto((PreVoteRequest) message)).build().toByteArray();
        }
        if (message instanceof PreVoteResponse) {
            return WireMessage.newBuilder().setPreVoteResponse(toProto((PreVoteResponse) message)).build().toByteArray();
        }
        if (message instanceof AcknowledgeEntries) {
            return WireMessage.newBuilder().setAcknowledgeEntries(toProto((AcknowledgeEntries) message)).build().toByteArray();
        }
        if (message instanceof AppendEntries) {
            return WireMessage.newBuilder().setAppendEntries(toProto((AppendEntries) message)).build().toByteArray();
        }
        if (message instanceof InstallSnapshot) {
            return WireMessage.newBuilder().setInstallSnapshot(toProto((InstallSnapshot) message)).build().toByteArray();
        }
        if (message instanceof InstallSnapshotResponse) {
            return WireMessage.newBuilder().setInstallSnapshotResponse(toProto((InstallSnapshotResponse) message)).build().toByteArray();
        }
        if (message instanceof Handshake) {
            return WireMessage.newBuilder().setHandshake(toProto((Handshake) message)).build().toByteArray();
        }
        if (message instanceof Acknowledgement) {
            return WireMessage.newBuilder().setAcknowledgement(com.github.jocull.raftcache.lib.raft.messages.proto.Acknowledgement.getDefaultInstance()).build().toByteArray();
        }
        if (message instanceof CachePutRequest) {
            return WireMessage.newBuilder().setCachePutRequest(toProto((CachePutRequest) message)).build().toByteArray();
        }
        if (message instanceof CacheDeleteRequest) {
            return WireMessage.newBuilder().setCacheDeleteRequest(toProto((CacheDeleteRequest) message)).build().toByteArray();
        }
        if (message instanceof CacheExpireRequest) {
            return WireMessage.newBuilder().setCacheExpireRequest(toProto((CacheExpireRequest) message)).build().toByteArray();
        }
        if (message instanceof CacheReadRequest) {
            return WireMessage.newBuilder().setCacheReadRequest(toProto((CacheReadRequest) message)).build().toByteArray();
        }
        if (message instanceof CacheWriteResponse) {
            return WireMessage.newBuilder().setCacheWriteResponse(toProto((CacheWriteResponse) message)).build().toByteArray();
        }
        if (message instanceof CacheReadResponse) {
            return WireMessage.newBuilder().setCacheReadResponse(toProto((CacheReadResponse) message)).build().toByteArray();
        }
        if (message instanceof KeyDataOutput) {
            // The read response (B §3.2) — a payload, not a frame: no envelope variant,
            // so the raw protocol-buffers bytes are the wire form. The transport
            // distinguishes it by call site (the reply to a registered read), not by type tag.
            return toProto((KeyDataOutput) message).toByteArray();
        }
        throw new IllegalArgumentException(
                "Not a wire type (none of the " + FRAME_VARIANTS + " frame variants): "
                        + message.getClass().getName()
                        + " — the closed wire set is documented on WireCodec");
    }

    /**
     * Decode one wire frame back to a JVM message of the exact wire type.
     *
     * @param bytes the frame bytes (as produced by {@link #serialize(Object)})
     * @return the decoded message — always one of the closed-set types
     * @throws com.google.protobuf.InvalidProtocolBufferException if the bytes are not a
     *                                                            well-formed wire message
     */
    @SuppressWarnings("deprecation")
    public static Object deserialize(byte[] bytes) throws com.google.protobuf.InvalidProtocolBufferException {
        WireMessage message = WireMessage.parseFrom(bytes);
        switch (message.getMessageCase()) {
            case INTRODUCTION:
                return fromProto(message.getIntroduction());
            case ANNOUNCE_CLUSTER_TOPOLOGY:
                return fromProto(message.getAnnounceClusterTopology());
            case STATE_REQUEST:
                return fromProto(message.getStateRequest());
            case STATE_RESPONSE:
                return fromProto(message.getStateResponse());
            case VOTE_REQUEST:
                return fromProto(message.getVoteRequest());
            case VOTE_RESPONSE:
                return fromProto(message.getVoteResponse());
            case PRE_VOTE_REQUEST:
                return fromProto(message.getPreVoteRequest());
            case PRE_VOTE_RESPONSE:
                return fromProto(message.getPreVoteResponse());
            case ACKNOWLEDGE_ENTRIES:
                return fromProto(message.getAcknowledgeEntries());
            case APPEND_ENTRIES:
                return fromProto(message.getAppendEntries());
            case INSTALL_SNAPSHOT:
                return fromProto(message.getInstallSnapshot());
            case INSTALL_SNAPSHOT_RESPONSE:
                return fromProto(message.getInstallSnapshotResponse());
            case HANDSHAKE:
                return fromProto(message.getHandshake());
            case ACKNOWLEDGEMENT:
                return new Acknowledgement();
            case CACHE_PUT_REQUEST:
                return fromProto(message.getCachePutRequest());
            case CACHE_DELETE_REQUEST:
                return fromProto(message.getCacheDeleteRequest());
            case CACHE_EXPIRE_REQUEST:
                return fromProto(message.getCacheExpireRequest());
            case CACHE_READ_REQUEST:
                return fromProto(message.getCacheReadRequest());
            case CACHE_WRITE_RESPONSE:
                return fromProto(message.getCacheWriteResponse());
            case CACHE_READ_RESPONSE:
                return fromProto(message.getCacheReadResponse());
            case MESSAGE_NOT_SET:
            default:
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        "wire frame carried no message variant (MESSAGE_NOT_SET)");
        }
    }

    /**
     * Decode a {@code KeyDataOutput} frame (the read reply, B §3.2) — the counterpart
     * of the {@link #serialize(Object)} branch for the read payload.
     */
    public static KeyDataOutput decodeKeyDataOutput(byte[] bytes) throws com.google.protobuf.InvalidProtocolBufferException {
        return fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataOutput.parseFrom(bytes));
    }

    // ---------------------------------------------------------------------
    // to proto
    // ---------------------------------------------------------------------

    private static com.github.jocull.raftcache.lib.raft.messages.proto.NodeAddress toProto(NodeAddress address) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.NodeAddress.newBuilder()
                .setAddress(address.getAddress())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.TermIndex toProto(TermIndex termIndex) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.TermIndex.newBuilder()
                .setTerm(termIndex.getTerm())
                .setIndex(termIndex.getIndex())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.NodeIdentifier toProto(NodeIdentifier id) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.NodeIdentifier.newBuilder()
                .setId(id.getId())
                .setNodeAddress(toProto(id.getNodeAddress()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.NodeState toProto(NodeStates state) {
        return switch (state) {
            case FOLLOWER -> com.github.jocull.raftcache.lib.raft.messages.proto.NodeState.NODE_STATE_FOLLOWER;
            case CANDIDATE -> com.github.jocull.raftcache.lib.raft.messages.proto.NodeState.NODE_STATE_CANDIDATE;
            case LEADER -> com.github.jocull.raftcache.lib.raft.messages.proto.NodeState.NODE_STATE_LEADER;
        };
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataInput toProto(KeyDataInput input) {
        com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataInput.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataInput.newBuilder().setKey(input.getKey());
        input.getData().forEach((name, value) ->
                builder.putData(name, ByteString.copyFrom(value == null ? new byte[0] : value)));
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataDeleteInput toProto(KeyDataDeleteInput input) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataDeleteInput.newBuilder()
                .setKey(input.getKey())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataExpireInput toProto(KeyDataExpireInput input) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataExpireInput.newBuilder()
                .setKey(input.getKey())
                .setTtlNanos(input.getTtl().toNanos())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataOutput toProto(KeyDataOutput output) {
        com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataOutput.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataOutput.newBuilder()
                        .setKey(output.getKey())
                        .setVersion(output.getVersion());
        output.getData().forEach((name, value) ->
                builder.putData(name, ByteString.copyFrom(value == null ? new byte[0] : value)));
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog toProto(AppendEntries.RaftLog entry) {
        com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog.newBuilder()
                        .setTermIndex(toProto(entry.getTermIndex()));
        Object value = entry.getEntry();
        if (value instanceof KeyDataInput) {
            builder.setKeyDataPut(toProto((KeyDataInput) value));
        } else if (value instanceof KeyDataDeleteInput) {
            builder.setKeyDataDelete(toProto((KeyDataDeleteInput) value));
        } else if (value instanceof KeyDataExpireInput) {
            builder.setKeyDataExpire(toProto((KeyDataExpireInput) value));
        } else if (value instanceof String) {
            builder.setStringEntry((String) value);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported RaftLog entry type: " + value.getClass().getName()
                            + " — the state machine applies KeyDataInput / KeyDataDeleteInput /"
                            + " KeyDataExpireInput / String entries only (see wire.proto RaftLog.oneof entry)");
        }
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.Introduction toProto(Introduction introduction) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.Introduction.newBuilder()
                .setId(introduction.getId())
                .setNodeAddress(toProto(introduction.getNodeAddress()))
                .setLastReceivedTermIndex(toProto(introduction.getLastReceivedTermIndex()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.AnnounceClusterTopology toProto(AnnounceClusterTopology announce) {
        com.github.jocull.raftcache.lib.raft.messages.proto.AnnounceClusterTopology.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.AnnounceClusterTopology.newBuilder();
        announce.getNodeIdentifierStates().forEach(id -> builder.addIdentifiers(toProto(id)));
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.StateRequest toProto(StateRequest request) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.StateRequest.newBuilder()
                .setRequestId(request.getRequestId().toString())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.StateResponse toProto(StateResponse response) {
        com.github.jocull.raftcache.lib.raft.messages.proto.StateResponse.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.StateResponse.newBuilder()
                        .setRequestId(response.getRequestId().toString())
                        .setIdentifier(toProto(new NodeIdentifier(
                                response.getIdentifier().getId(),
                                response.getIdentifier().getNodeAddress())))
                        .setState(toProto(response.getState()))
                        .setTerm(response.getTerm())
                        .setCurrentTermIndex(toProto(response.getCurrentTermIndex()))
                        .setCommittedTermIndex(toProto(response.getCommittedTermIndex()));
        // Client B §4.2 (2026-11-25): share the cluster view (additive proto field 7).
        response.getCluster().forEach(address -> builder.addCluster(toProto(address)));
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.VoteRequest toProto(VoteRequest vote) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.VoteRequest.newBuilder()
                .setTerm(vote.getTerm())
                .setCandidateId(vote.getCandidateId())
                .setLastLogTermIndex(toProto(vote.getLastLogTermIndex()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.VoteResponse toProto(VoteResponse response) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.VoteResponse.newBuilder()
                .setTerm(response.getTerm())
                .setCommittedTermIndex(toProto(response.getCommittedTermIndex()))
                .setVoteGranted(response.isVoteGranted())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteRequest toProto(PreVoteRequest preVote) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteRequest.newBuilder()
                .setTerm(preVote.getTerm())
                .setCandidateId(preVote.getCandidateId())
                .setLastLogTermIndex(toProto(preVote.getLastLogTermIndex()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteResponse toProto(PreVoteResponse response) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPreVoteGranted(response.isPreVoteGranted())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.AcknowledgeEntries toProto(AcknowledgeEntries ack) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.AcknowledgeEntries.newBuilder()
                .setTerm(ack.getTerm())
                .setSuccess(ack.isSuccess())
                .setCurrentTermIndex(toProto(ack.getCurrentTermIndex()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.AppendEntries toProto(AppendEntries append) {
        com.github.jocull.raftcache.lib.raft.messages.proto.AppendEntries.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.AppendEntries.newBuilder()
                        .setTerm(append.getTerm())
                        .setPreviousLogTermIndex(toProto(append.getPreviousLogTermIndex()))
                        .setLeaderCommitTermIndex(toProto(append.getLeaderCommitTermIndex()));
        append.getEntries().forEach(entry -> builder.addEntries(toProto(entry)));
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshot toProto(InstallSnapshot snapshot) {
        if (!(snapshot.getPayload() instanceof byte[])) {
            throw new IllegalArgumentException(
                    "InstallSnapshot payload must be a byte[] chunk (the leader's serialized state-machine"
                    + " capture). Got: " + snapshot.getPayload().getClass().getName());
        }
        return com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshot.newBuilder()
                .setTerm(snapshot.getTerm())
                .setLeaderId(snapshot.getLeaderId())
                .setLastIncludedIndex(snapshot.getLastIncludedIndex())
                .setLastIncludedTerm(snapshot.getLastIncludedTerm())
                .setOffset(snapshot.getOffset())
                .setPayload(ByteString.copyFrom((byte[]) snapshot.getPayload()))
                .setDone(snapshot.isDone())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshotResponse toProto(InstallSnapshotResponse response) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshotResponse.newBuilder()
                .setTerm(response.getTerm())
                .setIndex(response.getIndex())
                .setOffset(response.getOffset())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.Handshake toProto(Handshake handshake) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.Handshake.newBuilder()
                .setClient(toProto(handshake.getClient()))
                .setTarget(toProto(handshake.getTarget()))
                .build();
    }

    // Client B §3.1 (2026-11-25): the six client cache verb frames.

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheStatus toProto(CacheStatus status) {
        return switch (status) {
            case UNSPECIFIED -> com.github.jocull.raftcache.lib.raft.messages.proto.CacheStatus.CACHE_STATUS_UNSPECIFIED;
            case OK -> com.github.jocull.raftcache.lib.raft.messages.proto.CacheStatus.CACHE_STATUS_OK;
            case NOT_LEADER -> com.github.jocull.raftcache.lib.raft.messages.proto.CacheStatus.CACHE_STATUS_NOT_LEADER;
        };
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CachePutRequest toProto(CachePutRequest request) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.CachePutRequest.newBuilder()
                .setRequestId(request.getRequestId().toString())
                .setPayload(toProto(request.getPayload()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheDeleteRequest toProto(CacheDeleteRequest request) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.CacheDeleteRequest.newBuilder()
                .setRequestId(request.getRequestId().toString())
                .setPayload(toProto(request.getPayload()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheExpireRequest toProto(CacheExpireRequest request) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.CacheExpireRequest.newBuilder()
                .setRequestId(request.getRequestId().toString())
                .setPayload(toProto(request.getPayload()))
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadRequest toProto(CacheReadRequest request) {
        return com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadRequest.newBuilder()
                .setRequestId(request.getRequestId().toString())
                .setKey(request.getKey())
                .build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheWriteResponse toProto(CacheWriteResponse response) {
        com.github.jocull.raftcache.lib.raft.messages.proto.CacheWriteResponse.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.CacheWriteResponse.newBuilder()
                        .setRequestId(response.getRequestId().toString())
                        .setStatus(toProto(response.getStatus()));
        if (response.getLeaderHint() != null) {
            builder.setLeaderHint(toProto(response.getLeaderHint()));
        }
        return builder.build();
    }

    private static com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadResponse toProto(CacheReadResponse response) {
        com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadResponse.Builder builder =
                com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadResponse.newBuilder()
                        .setRequestId(response.getRequestId().toString())
                        .setFound(response.isFound());
        if (response.getValue() != null) {
            builder.setValue(toProto(response.getValue()));
        }
        return builder.build();
    }

    // ---------------------------------------------------------------------
    // from proto
    // ---------------------------------------------------------------------

    private static NodeAddress fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.NodeAddress address) {
        return new NodeAddress(address.getAddress());
    }

    private static TermIndex fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.TermIndex termIndex) {
        return new TermIndex(termIndex.getTerm(), termIndex.getIndex());
    }

    private static NodeStates fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.NodeState state) {
        return switch (state) {
            case NODE_STATE_FOLLOWER -> NodeStates.FOLLOWER;
            case NODE_STATE_CANDIDATE -> NodeStates.CANDIDATE;
            case NODE_STATE_LEADER -> NodeStates.LEADER;
            default -> throw new IllegalArgumentException(
                    "Unknown node state on the wire: " + state + " — a well-formed StateResponse carries one of FOLLOWER/CANDIDATE/LEADER");
        };
    }

    private static Map<String, byte[]> fromProto(Map<String, ByteString> data) {
        Map<String, byte[]> result = new java.util.LinkedHashMap<>();
        data.forEach((name, bytes) -> result.put(name, bytes.toByteArray()));
        return Map.copyOf(result);
    }

    private static com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.NodeIdentifier id) {
        return new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(id.getId(), fromProto(id.getNodeAddress()));
    }

    private static Introduction fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.Introduction introduction) {
        return new Introduction(introduction.getId(), fromProto(introduction.getNodeAddress()),
                fromProto(introduction.getLastReceivedTermIndex()));
    }

    private static AnnounceClusterTopology fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.AnnounceClusterTopology announce) {
        List<NodeIdentifier> identifiers = new ArrayList<>(announce.getIdentifiersCount());
        announce.getIdentifiersList().forEach(id -> identifiers.add(fromProto(id)));
        return new AnnounceClusterTopology(identifiers);
    }

    private static StateRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.StateRequest request) {
        return new StateRequest(UUID.fromString(request.getRequestId()));
    }

    private static StateResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.StateResponse response) {
        // The constructor derives the lib.topology NodeIdentifier from id+address — mirror
        // the construction, not the fields (there is no setter surface to fill).
        com.github.jocull.raftcache.lib.raft.messages.proto.NodeIdentifier wireId = response.getIdentifier();
        // Client B §4.2 (2026-11-25): cluster view (additive proto field 7; absent on
        // a pre-B sender, and the POJO constructor then defaults to an empty list).
        List<NodeAddress> cluster = new ArrayList<>(response.getClusterCount());
        response.getClusterList().forEach(address -> cluster.add(fromProto(address)));
        return new StateResponse(new Request(UUID.fromString(response.getRequestId())),
                wireId.getId(), fromProto(wireId.getNodeAddress()),
                fromProto(response.getState()),
                response.getTerm(),
                fromProto(response.getCurrentTermIndex()),
                fromProto(response.getCommittedTermIndex()),
                cluster);
    }

    // Client B §3.1 (2026-11-25): the six client cache verb frames.

    private static CacheStatus fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheStatus status) {
        return switch (status) {
            case CACHE_STATUS_UNSPECIFIED -> CacheStatus.UNSPECIFIED;
            case CACHE_STATUS_OK -> CacheStatus.OK;
            case CACHE_STATUS_NOT_LEADER -> CacheStatus.NOT_LEADER;
            default -> throw new IllegalArgumentException(
                    "Unknown cache status on the wire: " + status + " — a well-formed CacheWriteResponse carries UNSPECIFIED/OK/NOT_LEADER");
        };
    }

    private static CachePutRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CachePutRequest request) {
        return new CachePutRequest(UUID.fromString(request.getRequestId()), fromProto(request.getPayload()));
    }

    private static CacheDeleteRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheDeleteRequest request) {
        return new CacheDeleteRequest(UUID.fromString(request.getRequestId()), fromProto(request.getPayload()));
    }

    private static CacheExpireRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheExpireRequest request) {
        return new CacheExpireRequest(UUID.fromString(request.getRequestId()), fromProto(request.getPayload()));
    }

    private static CacheReadRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadRequest request) {
        return new CacheReadRequest(UUID.fromString(request.getRequestId()), request.getKey());
    }

    private static CacheWriteResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheWriteResponse response) {
        return new CacheWriteResponse(new Request(UUID.fromString(response.getRequestId())),
                fromProto(response.getStatus()),
                response.hasLeaderHint() ? fromProto(response.getLeaderHint()) : null);
    }

    private static CacheReadResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.CacheReadResponse response) {
        return new CacheReadResponse(new Request(UUID.fromString(response.getRequestId())),
                response.getFound(),
                response.hasValue() ? fromProto(response.getValue()) : null);
    }

    private static VoteRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.VoteRequest vote) {
        return new VoteRequest(vote.getTerm(), vote.getCandidateId(), fromProto(vote.getLastLogTermIndex()));
    }

    private static VoteResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.VoteResponse response) {
        return new VoteResponse(response.getTerm(), fromProto(response.getCommittedTermIndex()), response.getVoteGranted());
    }

    private static PreVoteRequest fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteRequest preVote) {
        return new PreVoteRequest(preVote.getTerm(), preVote.getCandidateId(), fromProto(preVote.getLastLogTermIndex()));
    }

    private static PreVoteResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.PreVoteResponse response) {
        return new PreVoteResponse(response.getTerm(), response.getPreVoteGranted());
    }

    private static AcknowledgeEntries fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.AcknowledgeEntries ack) {
        return new AcknowledgeEntries(ack.getTerm(), ack.getSuccess(), fromProto(ack.getCurrentTermIndex()));
    }

    private static AppendEntries.RaftLog fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog entry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog.EntryCase entryCase = entry.getEntryCase();
        Object value;
        switch (entryCase) {
            case KEY_DATA_PUT:
                value = fromProto(entry.getKeyDataPut());
                break;
            case KEY_DATA_DELETE:
                value = fromProto(entry.getKeyDataDelete());
                break;
            case KEY_DATA_EXPIRE:
                value = fromProto(entry.getKeyDataExpire());
                break;
            case STRING_ENTRY:
                value = entry.getStringEntry();
                break;
            case ENTRY_NOT_SET:
            default:
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        "RaftLog entry carried no value (ENTRY_NOT_SET)");
        }
        return new AppendEntries.RaftLog(fromProto(entry.getTermIndex()), value);
    }

    private static KeyDataInput fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataInput input) {
        return new KeyDataInput(input.getKey(), fromProto(input.getDataMap()));
    }

    private static KeyDataDeleteInput fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataDeleteInput input) {
        return new KeyDataDeleteInput(input.getKey());
    }

    private static KeyDataExpireInput fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataExpireInput input) {
        return new KeyDataExpireInput(input.getKey(), Duration.ofNanos(input.getTtlNanos()));
    }

    private static KeyDataOutput fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.KeyDataOutput output) {
        return new KeyDataOutput(output.getKey(), fromProto(output.getDataMap()), output.getVersion());
    }

    private static AppendEntries fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.AppendEntries append)
            throws com.google.protobuf.InvalidProtocolBufferException {
        List<AppendEntries.RaftLog> entries = new ArrayList<>(append.getEntriesCount());
        for (com.github.jocull.raftcache.lib.raft.messages.proto.RaftLog entry : append.getEntriesList()) {
            entries.add(fromProto(entry));
        }
        return new AppendEntries(append.getTerm(),
                fromProto(append.getPreviousLogTermIndex()),
                fromProto(append.getLeaderCommitTermIndex()),
                entries);
    }

    private static InstallSnapshot fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshot snapshot) {
        return new InstallSnapshot(snapshot.getTerm(), snapshot.getLeaderId(),
                snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                snapshot.getOffset(), snapshot.getPayload().toByteArray(), snapshot.getDone());
    }

    private static InstallSnapshotResponse fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.InstallSnapshotResponse response) {
        return new InstallSnapshotResponse(response.getTerm(), response.getIndex(), response.getOffset());
    }

    private static Handshake fromProto(com.github.jocull.raftcache.lib.raft.messages.proto.Handshake handshake) {
        return new Handshake(fromProto(handshake.getClient()), fromProto(handshake.getTarget()));
    }
}
