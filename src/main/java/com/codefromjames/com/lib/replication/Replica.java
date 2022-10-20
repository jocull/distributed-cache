package com.codefromjames.com.lib.replication;

import com.codefromjames.com.lib.event.EventBus;
import com.codefromjames.com.lib.event.EventSubscriber;
import com.codefromjames.com.lib.event.events.NewDataEvent;

public class Replica {
    private final EventBus eventBus;
    private final EventSubscriber newDataEventSubscriber;

    public Replica(EventBus eventBus) {
        this.eventBus = eventBus;
        this.newDataEventSubscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof NewDataEvent) {
                    // TODO: Do something here!
                }
            }
        };
    }

    public void startReplication() {
        eventBus.subscribe(newDataEventSubscriber);
    }
}
