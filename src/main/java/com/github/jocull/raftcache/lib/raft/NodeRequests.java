package com.github.jocull.raftcache.lib.raft;

import com.github.jocull.raftcache.lib.raft.messages.Request;
import com.github.jocull.raftcache.lib.raft.messages.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

class NodeRequests {
    private final Map<UUID, CompletableFuture<Response>> pendingRequests = new HashMap<>();

    public  <TRequest extends Request, TResponse extends Response> CompletableFuture<TResponse> getRequestFuture(TRequest request, Class<TResponse> tResponseClass) {
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
        return pendingResponse;
    }

    public void completeRequest(Response response) {
        final CompletableFuture<Response> pending;
        synchronized (pendingRequests) {
            pending = pendingRequests.get(response.getRequestId());
            if (pending == null) {
                // LOGGER.debug("{} State response from {} had no pending request {}", owner.getId(), remoteNodeId, response.getRequestId());
                return;
            }
        }
        pending.completeAsync(() -> response);
    }
}
