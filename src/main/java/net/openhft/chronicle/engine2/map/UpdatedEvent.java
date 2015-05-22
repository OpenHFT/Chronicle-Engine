package net.openhft.chronicle.engine2.map;


/**
 * Created by peter on 22/05/15.
 */
public class UpdatedEvent<K, V> implements MapEvent<K, V> {
    private final K key;
    private final V oldValue;
    private final V value;

    public UpdatedEvent(K key, V oldValue, V value) {
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
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
}
