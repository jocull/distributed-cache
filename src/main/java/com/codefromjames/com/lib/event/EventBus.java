package com.codefromjames.com.lib.event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventBus {
    private final List<EventSubscriber> subscribers = new CopyOnWriteArrayList<>();

    public void subscribe(EventSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe(EventSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    public void publish(Object event) {
        subscribers.forEach(s -> s.onEvent(event));
    }
}
