package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.View;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisher<T, M> extends View, Assetted<MapView<T, M, M>> {
    void publish(T topic, M message);

    void registerTopicSubscriber(TopicSubscriber<T, M> topicSubscriber) throws AssetNotFoundException;

    default boolean keyedView() {
        return true;
    }
}
