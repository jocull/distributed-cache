package com.github.jocull.raftcache.lib.event;

import com.github.jocull.raftcache.lib.event.events.SetDataEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class EventBusTest {
    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = new EventBus();
    }

    /** A null-tolerant subscriber: mirrors the house's production shape
     *  (Replica, RaftNodeBehaviorLeader's entriesSubscriber) that filters on
     *  {@code instanceof} and so silently swallows any non-matching event --
     *  INCLUDING a null one. Records every delivery so a refused fan-out is
     *  observable whether or not any subscriber happened to throw. */
    private static final class RecordingSubscriber extends EventSubscriber {
        final List<Object> received = new ArrayList<>();

        @Override
        public void onEvent(Object event) {
            received.add(event);
        }
    }

    private RecordingSubscriber newRecorder() {
        final RecordingSubscriber recorder = new RecordingSubscriber();
        eventBus.subscribe(recorder);
        return recorder;
    }

    @Test
    void testSubscribeRefusesNullSubscriber() {
        final RecordingSubscriber before = newRecorder();

        // A refused null subscriber sits on the list and crashes the fan-out at
        // the first legitimate publish (an NPE at dispatch, starving any
        // subscriber after it in list order)
        final RecordingSubscriber after = newRecorder();
        final SetDataEvent event = new SetDataEvent("k", Map.of("d", new byte[]{1}), 1L);

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> eventBus.subscribe(null));
        assertEquals("subscriber must not be null", refusal.getMessage(),
                "A nulled subscriber must name the subscriber");

        // Zero-state-change discriminator: the rejection must leave a functioning
        // bus -- both legitimate subscribers still served exactly the legitimate
        // event, no crash at dispatch
        eventBus.publish(event);
        assertEquals(List.of(event), before.received,
                "A subscribed-earlier subscriber must still be served after a refusal");
        assertEquals(List.of(event), after.received,
                "A subscribed-later subscriber must still be served after a refusal");
    }

    @Test
    void testUnsubscribeRefusesNullSubscriber() {
        final RecordingSubscriber before = newRecorder();

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> eventBus.unsubscribe(null));
        assertEquals("subscriber must not be null", refusal.getMessage(),
                "A nulled subscriber must name the subscriber");

        // Zero-state-change discriminator: the refusal must not touch the installed
        // subscriber -- it is still subscribed and still served
        final SetDataEvent event = new SetDataEvent("k", Map.of("d", new byte[]{1}), 1L);
        eventBus.publish(event);
        assertEquals(List.of(event), before.received,
                "A refusal must not uninstall or touch the installed subscribers");
    }

    @Test
    void testPublishRefusesNullEvent() {
        final RecordingSubscriber first = newRecorder();
        final RecordingSubscriber second = newRecorder();

        final IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> eventBus.publish(null));
        assertEquals("event must not be null", refusal.getMessage(),
                "A nulled event must name the event");

        // Zero-fan-out discriminator: the refusal must happen BEFORE any subscriber
        // is invoked -- the null event must not ride to the null-tolerant
        // instanceof-shaped subscribers (silently swallowed) and only be refused
        // deep -- by the sealed StateMachineBridge intake -- mid-iteration,
        // aborting delivery to the subscribers after it
        assertTrue(first.received.isEmpty(),
                "A refused publish must not fan out to the first subscriber");
        assertTrue(second.received.isEmpty(),
                "A refused publish must not fan out past any subscriber");
    }

    @Test
    void testLegitimateIntakeStillWorks() {
        final RecordingSubscriber recorder = newRecorder();
        final SetDataEvent event = new SetDataEvent("k", Map.of("d", new byte[]{1}), 1L);

        eventBus.publish(event);
        assertEquals(List.of(event), recorder.received,
                "A subscribed subscriber must be served exactly once per publish");

        eventBus.unsubscribe(recorder);
        eventBus.publish(event);
        assertEquals(List.of(event), recorder.received,
                "An unsubscribed subscriber must not be served again");
    }
}
