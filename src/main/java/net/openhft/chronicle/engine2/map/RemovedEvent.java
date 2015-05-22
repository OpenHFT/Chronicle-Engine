package net.openhft.chronicle.engine2.map;

/**
 * Created by peter on 22/05/15.
 */
public class RemovedEvent<K, V> implements MapEvent<K, V> {
    private final K key;
    private final V value;

    public RemovedEvent(K key, V value) {
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
        listener.remove(key, value);
    }
}
