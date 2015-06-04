package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.api.map.MapView;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisher<T, M> extends View, Assetted<MapView<T, M, M>> {
    void publish(T topic, M message);

    void registerTopicSubscriber(TopicSubscriber<T, M> topicSubscriber);

    default boolean keyedView() {
        return true;
    }
}
