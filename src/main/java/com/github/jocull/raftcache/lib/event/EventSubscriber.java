package com.github.jocull.raftcache.lib.event;

public abstract class EventSubscriber {
    public abstract void onEvent(Object event);
}
