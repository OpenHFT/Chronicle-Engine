package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription {
    <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query);

    <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query);

    <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query);

    <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query);
}
