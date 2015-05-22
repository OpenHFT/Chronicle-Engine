package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.KeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public interface MapEvent<K, V> extends KeyValueStore.Entry<K, V> {
    void apply(MapEventListener<K, V> listener);
}
