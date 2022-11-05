package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.*;
import com.github.jocull.raftcache.lib.raft.middleware.ChannelMiddleware;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import com.github.jocull.raftcache.lib.topology.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NodeCommunication {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCommunication.class);

    private final RaftNode owner;
    private final ChannelMiddleware.ChannelSide channel;

    private String remoteNodeId; // Unknown until introduction
    private volatile long currentIndex;
    // TODO: In addition to the `currentIndex`, the `term` is likely important to track here too...?
    //       Are we able to make sense of the `term` coming out of each message and keep track of it here
    //       to help indicate what term the remote node is in? This can be used to announce cluster topology.

    private final Map<UUID, CompletableFuture<Response>> pendingRequests = new HashMap<>();

    public NodeCommunication(RaftNode owner,
                             ChannelMiddleware.ChannelSide channel) {
        this.owner = owner;
        this.channel = channel;

        // Register yourself into this view
        this.owner.getClusterTopology().register(new NodeIdentifier(owner.getId(), owner.getNodeAddress()));

        this.channel.setReceiver(this::receive);
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    public NodeAddress getRemoteNodeAddress() {
        return channel.getAddress();
    }

    public long getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(long currentIndex) {
        this.currentIndex = currentIndex;
    }

    private void send(Object message) {
        channel.send(message);
    }

    // TODO: Initial lazy version, not very maintainable with growing number of message types
    private void receive(Object message) {
        // Introductions must be completed first to establish node IDs
        if (message instanceof Introduction) {
            onIntroduction((Introduction) message);
            return;
        }
        if (message instanceof AnnounceClusterTopology) {
            onAnnounceClusterTopology((AnnounceClusterTopology) message);
            return;
        }
        if (message instanceof StateRequest) {
            onStateRequest((StateRequest) message);
            return;
        }
        if (message instanceof StateResponse) {
            onStateResponse((StateResponse) message);
            return;
        }
        if (remoteNodeId == null) {
            throw new IllegalStateException("Cannot process " + message.getClass().getSimpleName()
                    + " before introduction! remoteNodeId is null");
        }

        // After introductions, any message can process
        if (message instanceof VoteRequest) {
            onVoteRequest((VoteRequest) message);
        } else if (message instanceof VoteResponse) {
            onVoteResponse((VoteResponse) message);
        } else if (message instanceof AppendEntries) {
            onAppendEntries((AppendEntries) message);
        } else if (message instanceof AcknowledgeEntries) {
            onAcknowledgeEntries((AcknowledgeEntries) message);
        }
    }

    private <TRequest extends Request, TResponse extends Response> CompletableFuture<TResponse> request(TRequest request, Class<TResponse> tResponseClass) {
        // Prepare the request's timeout
        final Executor delayedExecutor = CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS); // TODO: Is this timeout appropriate?
        final CompletableFuture<Void> requestCancellation = CompletableFuture.runAsync(() -> {
            synchronized (pendingRequests) {
                final CompletableFuture<?> requestToCancel = pendingRequests.remove(request.getRequestId());
                if (requestToCancel != null) {
                    ForkJoinPool.commonPool().submit(() -> requestToCancel.cancel(false));
                }
            }
        }, delayedExecutor);
        // Setup the pending response future which will be filled
        final CompletableFuture<TResponse> pendingResponse = new CompletableFuture<TResponse>()
                .thenApply(response -> {
                    Objects.requireNonNull(response, "Response cannot be null!");
                    if (!response.getClass().isAssignableFrom(tResponseClass)) {
                        throw new ClassCastException(response.getClass().getName() + " is not compatible with " + tResponseClass.getName());
                    }
                    return response;
                });
        // When pending requests complete, cancel the cancellation task and remove the pending entry immediately.
        // This future isn't chained on purpose because it's not part of the future we want to return - it's a side effect.
        pendingResponse.thenRun(() -> {
            requestCancellation.cancel(false);
            pendingRequests.remove(request.getRequestId());
        });
        // Register the outgoing request future into the tracking map before sending the request out
        synchronized (pendingRequests) {
            //noinspection unchecked
            pendingRequests.put(request.getRequestId(), (CompletableFuture<Response>) pendingResponse);
        }
        send(request);
        return pendingResponse;
    }

    public void introduce() {
        send(new Introduction(
                owner.getId(),
                owner.getNodeAddress(),
                owner.getLastReceivedIndex()
        ));
    }

    private synchronized void onIntroduction(Introduction introduction) {
        // We register data about the node that has introduced itself
        owner.getClusterTopology().register(new NodeIdentifier(
                introduction.getId(),
                introduction.getNodeAddress()
        ));
        remoteNodeId = introduction.getId();

        // And reply with the cluster topology as we know it
        send(new AnnounceClusterTopology(
                owner.getClusterTopology().getTopology().stream()
                        .map(i -> new com.github.jocull.raftcache.lib.raft.messages.NodeIdentifier(
                                i.getId(),
                                i.getNodeAddress()
                        ))
                        .collect(Collectors.toList())
        ));
    }

    private void onAnnounceClusterTopology(AnnounceClusterTopology announceClusterTopology) {
        // When the topology has been received we can update our local view of the world
        owner.getClusterTopology().register(announceClusterTopology.getNodeIdentifierStates().stream()
                .map(i -> new NodeIdentifier(
                        i.getId(),
                        i.getNodeAddress()
                ))
                .collect(Collectors.toList()));

        if (remoteNodeId == null) {
            remoteNodeId = owner.getClusterTopology().locate(channel.getAddress())
                    .map(com.github.jocull.raftcache.lib.topology.NodeIdentifier::getId)
                    .orElseThrow(() -> new IllegalStateException("Remote address " + channel.getAddress() + " not found in cluster toplogy: {}" + owner.getClusterTopology().getTopology()));
        }
    }

    public CompletableFuture<StateResponse> requestState() {
        return request(new StateRequest(), StateResponse.class);
    }

    private void onStateRequest(StateRequest stateRequest) {
        send(owner.onStateRequest(stateRequest));
    }

    private void onStateResponse(StateResponse stateResponse) {
        final CompletableFuture<Response> pending;
        synchronized (pendingRequests) {
            pending = pendingRequests.get(stateResponse.getRequestId());
            if (pending == null) {
                LOGGER.debug("{} State response from {} had no pending request {}", owner.getId(), remoteNodeId, stateResponse.getRequestId());
                return;
            }
        }
        pending.completeAsync(() -> stateResponse);
    }

    public void requestVote(VoteRequest voteRequest) {
        send(voteRequest);
    }

    private void onVoteRequest(VoteRequest voteRequest) {
        owner.onVoteRequest(this, voteRequest)
                .ifPresent(this::send);
    }

    private void onVoteResponse(VoteResponse voteResponse) {
        owner.onVoteResponse(this, voteResponse);
    }

    public void appendEntries(AppendEntries appendEntries) {
        send(appendEntries);
    }

    private void onAppendEntries(AppendEntries appendEntries) {
        send(owner.onAppendEntries(this, appendEntries));
    }

    private void onAcknowledgeEntries(AcknowledgeEntries acknowledgeEntries) {
        owner.onAcknowledgeEntries(this, acknowledgeEntries);
    }
}
