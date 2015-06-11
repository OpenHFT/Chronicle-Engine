package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.map.EventConsumer;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription<T, E> extends View {
    void registerSubscriber(RequestContext rc, Subscriber subscriber);

    void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber);

    void unregisterSubscriber(RequestContext rc, Subscriber subscriber);

    void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber);

    void registerDownstream(EventConsumer<T, E> subscription);
}
