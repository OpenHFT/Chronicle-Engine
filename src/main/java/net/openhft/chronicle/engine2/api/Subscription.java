package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription {
    <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber);

    <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber);

    <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber);

    <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber);
}
