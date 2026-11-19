package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies committed Raft logs to a state machine ({@link PartitionMap}).
 *
 * <p>Raft nodes publish {@link LogsCommitted} on the node event bus whenever their commit
 * index moves forward -- both leaders and followers do this for the same committed entries.
 * This bridge subscribes to that bus and replays each newly committed log entry into the
 * state machine, tracking the highest index it has already applied. Entries at or below the
 * highest applied index are skipped, which makes delivery idempotent: in an in-process cluster
 * the same entry is committed (and published) by several nodes sharing one bus, and that is
 * applied exactly once.
 *
 * <p>{@link com.github.jocull.raftcache.lib.event.EventBus#publish(Object)} is synchronous,
 * so log entries are applied on the publisher's own thread -- the single-threaded-per-node
 * mailbox model is preserved and no additional executors are introduced.
 *
 * <p>Only {@link KeyDataInput} entries have state-machine semantics today; any other entry
 * type (e.g. plain strings used by the replication tests) is acknowledged and skipped.
 */
public class StateMachineBridge extends EventSubscriber implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineBridge.class);

    // Log indices start at 1 (TermIndex.EPOCH is term 0 index 0), so 0 means "nothing applied"
    private static final long NO_INDEX_APPLIED = 0L;

    private final EventBus eventBus;
    private final PartitionMap partitionMap;
    private final AtomicLong lastAppliedIndex = new AtomicLong(NO_INDEX_APPLIED);

    public StateMachineBridge(EventBus eventBus, PartitionMap partitionMap) {
        this.eventBus = eventBus;
        this.partitionMap = partitionMap;
        eventBus.subscribe(this);
    }

    @Override
    public void onEvent(Object event) {
        if (!(event instanceof LogsCommitted)) {
            return;
        }
        final LogsCommitted logsCommitted = (LogsCommitted) event;
        for (RaftLog<?> raftLog : logsCommitted.getLogs()) {
            applyLog(raftLog);
        }
        LOGGER.trace("Applied logs up to index {}", lastAppliedIndex.get());
    }

    private void applyLog(RaftLog<?> raftLog) {
        final long index = raftLog.getTermIndex().getIndex();
        if (index <= lastAppliedIndex.get()) {
            LOGGER.debug("Skipping log @ index {}: already applied (last applied index is {})", index, lastAppliedIndex.get());
            return;
        }

        final Object entry = raftLog.getEntry();
        if (entry instanceof KeyDataInput) {
            final KeyDataInput input = (KeyDataInput) entry;
            partitionMap.setData(input);
            LOGGER.debug("Applied log @ index {} (key={}) to the state machine", index, input.getKey());
        } else {
            // No state-machine handling for this entry type; acknowledge it by
            // advancing the applied index so later entries are not blocked behind it
            LOGGER.debug("No state-machine handling for entry type {} @ index {}", raftLog.getLogClass().getName(), index);
        }

        long previous = lastAppliedIndex.get();
        while (previous < index && !lastAppliedIndex.compareAndSet(previous, index)) {
            previous = lastAppliedIndex.get();
        }
    }

    /**
     * The highest log index applied to the state machine so far (0 if none).
     */
    public long getLastAppliedIndex() {
        return lastAppliedIndex.get();
    }

    @Override
    public void close() {
        eventBus.unsubscribe(this);
    }
}
