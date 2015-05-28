package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription {
    <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber);

    <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber);

    default void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
        unregisterSubscriber(rc.type(), subscriber, rc.toString());
    }
    <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query);

    default void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        unregisterTopicSubscriber(rc.type(), rc.type2(), subscriber, rc.toString());
    }

    <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query);
}
