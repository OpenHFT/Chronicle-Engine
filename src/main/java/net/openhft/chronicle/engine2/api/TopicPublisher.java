package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.engine2.api.map.KeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisher<M> extends View, Assetted<KeyValueStore<String, M, M>> {
    void publish(String topic, M message);
}
