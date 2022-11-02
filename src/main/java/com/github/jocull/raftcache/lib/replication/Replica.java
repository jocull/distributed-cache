package com.github.jocull.raftcache.lib.replication;

import com.github.jocull.raftcache.lib.event.EventBus;
import com.github.jocull.raftcache.lib.event.EventSubscriber;
import com.github.jocull.raftcache.lib.event.events.SetDataEvent;

public class Replica {
    private final EventBus eventBus;
    private final EventSubscriber newDataEventSubscriber;

    public Replica(EventBus eventBus) {
        this.eventBus = eventBus;
        this.newDataEventSubscriber = new EventSubscriber() {
            @Override
            public void onEvent(Object event) {
                if (event instanceof SetDataEvent) {
                    // TODO: Do something here!
                }
            }
        };
    }

    public void startReplication() {
        eventBus.subscribe(newDataEventSubscriber);
    }
}
