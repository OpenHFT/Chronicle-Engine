package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.TopicSubscriber;
import net.openhft.chronicle.engine.api.View;

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
