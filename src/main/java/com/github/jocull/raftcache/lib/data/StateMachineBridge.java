package com.github.jocull.raftcache.lib.data;

import com.github.jocull.raftcache.lib.communication.KeyDataDeleteInput;
import com.github.jocull.raftcache.lib.communication.KeyDataExpireInput;
import com.github.jocull.raftcache.lib.communication.KeyDataInput;
import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.raft.RaftLog;
import com.github.jocull.raftcache.lib.raft.TermIndex;
import com.github.jocull.raftcache.lib.raft.events.LogsCommitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>Entries of type {@link KeyDataInput} (SET), {@link KeyDataDeleteInput} (DELETE) and
 * {@link KeyDataExpireInput} (EXPIRE) have state-machine semantics; any other entry type
 * (e.g. plain strings used by the replication tests) is acknowledged and skipped.
 *
 * <p>Snapshot support (SNAPSHOTS.md stage 1, capture/replace): {@link #getSnapshot()}
 * captures the current state bound to the last applied log position (Raft §5.2.2's
 * {@code lastIncludedTerm}/{@code lastIncludedIndex}), and {@link #adopt(Snapshot)}
 * reinstalls that state on this node, re-anchoring the applied position so the replay
 * guard then skips exactly the entries the snapshot's state already reflects.
 */
public class StateMachineBridge extends EventSubscriber implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineBridge.class);

    // Log indices start at 1 (TermIndex.EPOCH is term 0, index 0), so (0, 0) means
    // "nothing applied". Term and index are held as one pair so the snapshot boundary
    // ({@link #getSnapshot()}) always reports the term of the entry at the applied index.
    private static final TermIndex NO_POSITION_APPLIED = new TermIndex(0, 0L);

    private final EventBus eventBus;
    private final PartitionMap partitionMap;
    private final AtomicReference<TermIndex> lastApplied = new AtomicReference<>(NO_POSITION_APPLIED);

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
        LOGGER.trace("Applied logs up to index {}", lastApplied.get().getIndex());
    }

    private void applyLog(RaftLog<?> raftLog) {
        final TermIndex termIndex = raftLog.getTermIndex();
        final long index = termIndex.getIndex();
        if (index <= lastApplied.get().getIndex()) {
            LOGGER.debug("Skipping log @ index {}: already applied (last applied is {})", index, lastApplied.get());
            return;
        }

        final Object entry = raftLog.getEntry();
        if (entry instanceof KeyDataInput) {
            final KeyDataInput input = (KeyDataInput) entry;
            partitionMap.setData(input);
            LOGGER.debug("Applied log @ index {} (key={}) to the state machine", index, input.getKey());
        } else if (entry instanceof KeyDataDeleteInput) {
            final KeyDataDeleteInput input = (KeyDataDeleteInput) entry;
            partitionMap.deleteData(input.getKey());
            LOGGER.debug("Applied delete log @ index {} (key={}) to the state machine", index, input.getKey());
        } else if (entry instanceof KeyDataExpireInput) {
            final KeyDataExpireInput input = (KeyDataExpireInput) entry;
            final boolean updated = partitionMap.expireData(input);
            LOGGER.debug("Applied expire log @ index {} (key={}, ttl={}, live entry={})", index, input.getKey(), input.getTtl(), updated);
        } else {
            // No state-machine handling for this entry type; acknowledge it by
            // advancing the applied index so later entries are not blocked behind it
            LOGGER.debug("No state-machine handling for entry type {} @ index {}", raftLog.getLogClass().getName(), index);
        }

        TermIndex current = lastApplied.get();
        while (current.getIndex() < index && !lastApplied.compareAndSet(current, termIndex)) {
            current = lastApplied.get();
        }
    }

    /**
     * The highest log index applied to the state machine so far (0 if none).
     */
    public long getLastAppliedIndex() {
        return lastApplied.get().getIndex();
    }

    /**
     * The term of the entry at the last applied index (0 if nothing has been applied).
     */
    public long getLastAppliedTerm() {
        return lastApplied.get().getTerm();
    }

    /**
     * Captures a point-in-time {@link Snapshot} (SNAPSHOTS.md stage 1): the full data
     * state ({@link PartitionMap#capture()}) bound to the last applied log position --
     * the {@code lastIncludedTerm}/{@code lastIncludedIndex} a future
     * {@code InstallSnapshot} message (stages 3-6) will carry (Raft §5.2.2).
     *
     * <p>The capture is a pure function of the state (invariant S4): the data and the
     * version counters it already carries, plus absolute expirations -- no local clock
     * reading is embedded, so the snapshot restores identically on any node.
     */
    public Snapshot getSnapshot() {
        final TermIndex position = lastApplied.get();
        return new Snapshot(position.getTerm(), position.getIndex(), partitionMap.capture());
    }

    /**
     * Installs a {@link Snapshot} onto this state machine (SNAPSHOTS.md stage 1,
     * capture/replace -- the receiver half of stages 4-6): the state is replaced by
     * {@code snapshot}'s entries, and the applied position is re-anchored to the
     * snapshot's boundary, so the replay guard then skips exactly the entries the
     * snapshot's state already reflects (no re-application of what it contains).
     *
     * <p>Progress-only and forward-only: a snapshot whose index is below the last applied
     * index is refused (invariants S2/S5 -- the applied position only ever moves
     * forward), and at the boundary the install is simply an idempotent re-install of
     * the same state.
     *
     * <p>Expected to be called on the adopting node's mailbox thread with no in-flight
     * applications (the same single-threaded-per-node model the event bus preserves);
     * concurrent applications of indices at or below the boundary are idempotently
     * skipped by the applied-index guard either way.
     *
     * @throws NullPointerException if {@code snapshot} is null
     */
    public void adopt(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        final TermIndex position = lastApplied.get();
        if (snapshot.index() < position.getIndex()) {
            LOGGER.info("Refusing to adopt a snapshot (term={}, index={}, entries={}) behind the last applied position {} -- the applied position only moves forward",
                    snapshot.term(), snapshot.index(), snapshot.size(), position);
            return;
        }
        partitionMap.restore(snapshot.entries());
        lastApplied.set(new TermIndex((int) snapshot.term(), snapshot.index()));
        LOGGER.info("Installed snapshot (term={}, index={}, entries={}) on the state machine",
                snapshot.term(), snapshot.index(), snapshot.size());
    }

    @Override
    public void close() {
        eventBus.unsubscribe(this);
    }
}
