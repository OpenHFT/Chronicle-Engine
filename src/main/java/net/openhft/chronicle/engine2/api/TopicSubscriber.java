package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicSubscriber<E> {
    void on(String name, E e);
}
