package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Interceptor;
import net.openhft.chronicle.engine2.api.TopicPublisher;

/**
 * Created by peter on 22/05/15.
 */
public interface TopicPublisherFactory<V> extends Interceptor {
    TopicPublisher<V> create(Asset asset, KeyValueStore<String, V> kvStore, String queryString);
}
