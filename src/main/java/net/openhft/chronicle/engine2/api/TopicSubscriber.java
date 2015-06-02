package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicSubscriber<T, M> extends ISubscriber {
    void onMessage(T topic, M message) throws InvalidSubscriberException;
}
