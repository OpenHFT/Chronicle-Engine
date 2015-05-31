package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisher<T, M> extends View, Assetted<SubscriptionKeyValueStore<T, M, M>> {
    void publish(T topic, M message);

    void registerSubscriber(TopicSubscriber<T, M> topicSubscriber);

    default boolean keyedView() {
        return true;
    }
}
