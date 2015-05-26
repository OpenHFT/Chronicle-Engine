package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.engine2.api.map.KeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisher<T, M> extends View, Assetted<KeyValueStore<T, M, M>> {
    void publish(T topic, M message);
}
