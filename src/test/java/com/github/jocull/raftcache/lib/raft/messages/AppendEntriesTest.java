package com.github.jocull.raftcache.lib.raft.messages;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the log-entry carrier (stage 7(b)).
 *
 * <p>The {@code RaftLog} constructor refuses a null {@code entry} — but pre-fix in the
 * pre-campaign style: a bare messageless {@code Objects.requireNonNull(entry)}, so the
 * axis was never named. The sibling {@code termIndex} axis is deliberately bare at the
 * carrier level (the receiver seams refuse it — the 6-series hostile-entry pins build
 * {@code RaftLog(null, "hostile-entry")} on purpose); 7(b) names only the axis that is
 * already guarded, keeping every carrier-level acceptance the pinned hostile shapes
 * depend on.
 */
public class AppendEntriesTest {

    // --- Non-happy-path intake pins (stage 7(b)): the constructor-carrier axis must be named ---

    @Test
    void testNullEntryNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the entry axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new AppendEntries.RaftLog(new TermIndex(1, 2L), null),
                "A null entry must be refused at the constructor");
        assertEquals("entry", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed log entry still builds and
        // its getters still read back.
        final TermIndex termIndex = new TermIndex(1, 2L);
        final AppendEntries.RaftLog log = new AppendEntries.RaftLog(termIndex, "entry-2");
        // messages.TermIndex is an identity type (no equals) — the getter must hand back
        // the very instance the carrier stored.
        assertSame(termIndex, log.getTermIndex());
        assertEquals("entry-2", log.getEntry());
    }
}
