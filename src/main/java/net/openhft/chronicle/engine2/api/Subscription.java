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

    void registerDownstream(Subscription subscription);

    void unregisterDownstream(Subscription subscription);

//    void unregisterAll(RequestContext rc);
}
