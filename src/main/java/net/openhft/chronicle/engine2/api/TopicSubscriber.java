package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicSubscriber<T, M> {
    void onMessage(T topic, M message);
}
