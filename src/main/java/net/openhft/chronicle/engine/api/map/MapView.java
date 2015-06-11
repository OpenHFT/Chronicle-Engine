package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.View;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public interface MapView<K, MV, V> extends ConcurrentMap<K, V>, Assetted<KeyValueStore<K, MV, V>>, View {
    default boolean keyedView() {
        return true;
    }

    void registerTopicSubscriber(TopicSubscriber<K, V> topicSubscriber);
}
