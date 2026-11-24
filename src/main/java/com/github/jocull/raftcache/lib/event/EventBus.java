package com.github.jocull.raftcache.lib.event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventBus {
    private final List<EventSubscriber> subscribers = new CopyOnWriteArrayList<>();

    public void subscribe(EventSubscriber subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("subscriber must not be null");
        }
        subscribers.add(subscriber);
    }

    public void unsubscribe(EventSubscriber subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("subscriber must not be null");
        }
        subscribers.remove(subscriber);
    }

    public void publish(Object event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        subscribers.forEach(s -> s.onEvent(event));
    }
}
