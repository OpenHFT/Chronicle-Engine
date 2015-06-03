package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.MapEventListener;
//import net.openhft.chronicle.hash.impl.util.Objects;

import java.util.Optional;

/**
 * Created by peter on 22/05/15.
 */
public class InsertedEvent<K, V> implements MapEvent<K, V> {
    private final K key;
    private final V value;

    private InsertedEvent(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> InsertedEvent<K, V> of(K key, V value) {
        return new InsertedEvent<>(key, value);
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

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();

    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "InsertedEvent{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
