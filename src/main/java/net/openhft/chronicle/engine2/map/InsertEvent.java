package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.MapEventListener;

/**
 * Created by peter on 22/05/15.
 */
public class InsertEvent<K, V> implements MapEvent<K, V> {
    private final K key;
    private final V value;

    public InsertEvent(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public void apply(MapEventListener<K, V> listener) {
        listener.insert(key, value);
    }
}
