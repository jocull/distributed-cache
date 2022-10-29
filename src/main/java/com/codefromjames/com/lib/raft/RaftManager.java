package com.codefromjames.com.lib.raft;

import com.codefromjames.com.lib.event.EventBus;
import com.codefromjames.com.lib.raft.middleware.ChannelMiddleware;
import com.codefromjames.com.lib.topology.NodeAddress;
import com.codefromjames.com.lib.topology.TopologyDiscovery;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftManager implements AutoCloseable {
    private static final AtomicInteger RAFT_MANAGER_COUNTER = new AtomicInteger();

    static final Random RANDOM = new Random();

    private final EventBus eventBus = new EventBus();
    private final TopologyDiscovery topologyDiscovery;
    private final ChannelMiddleware channelMiddleware;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    public RaftManager(TopologyDiscovery topologyDiscovery,
                       ChannelMiddleware channelMiddleware) {
        this.topologyDiscovery = topologyDiscovery;
        this.channelMiddleware = channelMiddleware;

        final int thisManagerCount = RAFT_MANAGER_COUNTER.getAndIncrement();
        final AtomicInteger threadCounter = new AtomicInteger();

        scheduledExecutor = new ScheduledThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors() * 2 ,
                r -> new Thread(r, "raft-manager-" + thisManagerCount + "-" + threadCounter.getAndIncrement()));
        scheduledExecutor.setRemoveOnCancelPolicy(true);
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduledExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @Override
    public void close() {
        scheduledExecutor.shutdownNow();
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    // region Channel Middleware delegates

    public ChannelMiddleware.ChannelSide openChannel(RaftNode node, NodeAddress targetAddress) {
        return channelMiddleware.openChannel(node, targetAddress);
    }

    // endregion

    // region Topology discovery delegates

    public List<NodeAddress> discoverNodes() {
        return topologyDiscovery.getNodes();
    }

    // endregion

    // region Executor delegates

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(command, delay, unit);
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(callable, delay, unit);
    }

    public Future<?> submit(Runnable task) {
        return scheduledExecutor.submit(task);
    }

    public <T> Future<T> submit(Callable<T> task) {
        return scheduledExecutor.submit(task);
    }

    // endregion
}
