package com.github.jocull.raftcache.lib.raft.messages;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Non-happy-path intake of the install-snapshot message carrier (stage 7(b)).
 *
 * <p>Since 6(g) this constructor is pinned as the message boundary — the class-level
 * pin {@code aNullPayload_isBlockedAtTheMessageBoundary_soTheFollowerNeverSeesOne}
 * (RaftNodeBehaviorFollowerInstallSnapshotTest) proves a null payload cannot be built.
 * But both null axes still spoke the pre-campaign style: bare messageless
 * {@code Objects.requireNonNull} calls — a refusal the caller could not distinguish
 * from a failure deep in the wiring. The 7(b) fix names both axes (leaderId, payload)
 * in the house constructor-carrier family; the 6(g) boundary invariant and its pin are
 * kept (the pin asserts the exception CLASS, so the naming does not move the guard).
 */
public class InstallSnapshotTest {

    // --- Non-happy-path intake pins (stage 7(b)): the constructor-carrier axes must be named ---

    @Test
    void testNullLeaderIdNamesAxis() {
        // Pre-fix: NPE thrown (the guard sat in the right place) but its message is null —
        // the leaderId axis was never named.
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new InstallSnapshot(0, null, 10L, 5, 0L, new byte[]{(byte) 0xAA}, true),
                "A null leader id must be refused at the constructor");
        assertEquals("leaderId", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testNullPayloadNamesAxis() {
        // Pre-fix: NPE thrown with a null message — the payload axis was never named.
        // (The 6(g) boundary pin already proves the null payload cannot be built; this
        // pin makes the refusal NAMED, in the house constructor-carrier family.)
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new InstallSnapshot(0, "leader", 10L, 5, 0L, null, true),
                "A null payload must be refused at the constructor");
        assertEquals("payload", ex.getMessage(),
                "A constructor carrier must name the dependency it refuses");
    }

    @Test
    void testValidConstructionSanity() {
        // The legitimate-intake discrimination: a well-formed message still builds and
        // its getters still read back.
        final byte[] chunk = new byte[]{(byte) 0xAA, (byte) 0xBB};
        final InstallSnapshot snapshot = new InstallSnapshot(0, "leader", 10L, 5, 0L, chunk, true);
        assertEquals(0, snapshot.getTerm());
        assertEquals("leader", snapshot.getLeaderId());
        assertEquals(10L, snapshot.getLastIncludedIndex());
        assertEquals(5, snapshot.getLastIncludedTerm());
        assertEquals(0L, snapshot.getOffset());
        // getPayload() is Object-typed (the stage-3 sketch's loose state-machine snapshot):
        // identity is what the carrier guarantees.
        assertSame(chunk, snapshot.getPayload());
        assertTrue(snapshot.isDone());
    }
}
