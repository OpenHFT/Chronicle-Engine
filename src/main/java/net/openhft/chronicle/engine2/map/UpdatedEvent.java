package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.MapEventListener;
import net.openhft.chronicle.hash.impl.util.Objects;

import java.util.Optional;

/**
 * Created by peter on 22/05/15.
 */
public class UpdatedEvent<K, V> implements MapEvent<K, V> {
    private final K key;
    private final V oldValue;
    private final V value;

    private UpdatedEvent(K key, V oldValue, V value) {
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
    }

    public static <K, V> UpdatedEvent<K, V> of(K key, V oldValue, V value) {
        return new UpdatedEvent<>(key, oldValue, value);
    }

    public K key() {
        return key;
    }

    public V oldValue() {
        return oldValue;
    }

    public V value() {
        return value;
    }

    @Override
    public void apply(MapEventListener<K, V> listener) {
        listener.update(key, oldValue, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash("updated", key, value);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof UpdatedEvent)
                .map(o -> (UpdatedEvent<K, V>) o)
                .filter(e -> Objects.equal(key, e.key))
                .filter(e -> Objects.equal(oldValue, e.oldValue))
                .filter(e -> Objects.equal(value, e.value))
                .isPresent();
    }

    @Override
    public String toString() {
        return "UpdatedEvent{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
