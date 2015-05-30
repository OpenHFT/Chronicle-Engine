package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription extends View {
    boolean hasSubscribers();

    <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber);

    <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber);

    void unregisterSubscriber(RequestContext rc, Subscriber subscriber);

    void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber);

    void registerDownstream(RequestContext rc, Subscription subscription);

    void unregisterDownstream(RequestContext rc, Subscription subscription);

//    void unregisterAll(RequestContext rc);
}
