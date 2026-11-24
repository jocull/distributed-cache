package com.github.jocull.raftcache.lib.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Non-happy-path intake of the raw log-entry carrier (stage 7(c)).
 *
 * <p>Pre-fix the constructor spoke the pre-campaign style: bare
 * {@code Objects.requireNonNull(termIndex)} / {@code (entry)} — NPEs with {@code null}
 * messages, so neither axis was ever named. The 7(c) fix moves both heads to the named
 * 2-arg form in the house constructor-carrier family; this carrier is the raw log-entry
 * type {@code StateMachineBridge} applies and {@code CompletableRaftLog} extends, so a
 * refused null was indistinguishable from a failure deep in the log wiring.
 */
public class RaftLogTest {

    // --- Non-happy-path intake pins (stage 7(c)): the constructor-carrier axes must be named ---

    @Test
    void testNullTermIndexNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the termIndex axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new RaftLog<String>(null, "entry"),
                "A null termIndex must be refused at the constructor");
        assertEquals("termIndex", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testNullEntryNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the entry axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new RaftLog<>(new TermIndex(1, 2L), null),
                "A null entry must be refused at the constructor");
        assertEquals("entry", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed entry still builds and
        // its getters still read back (a guard that refuses everything is as broken as
        // one that refuses nothing).
        final TermIndex termIndex = new TermIndex(1, 2L);
        final RaftLog<String> raftLog = new RaftLog<>(termIndex, "sanity-entry");
        assertSame(termIndex, raftLog.getTermIndex()); // TermIndex is an identity type — no equals
        assertEquals("sanity-entry", raftLog.getEntry());
        assertEquals(String.class, raftLog.getLogClass());
    }
}
