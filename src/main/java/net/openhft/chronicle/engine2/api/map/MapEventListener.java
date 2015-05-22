package net.openhft.chronicle.engine2.api.map;

public interface MapEventListener<K, V> {
    void update(K key, V oldValue, V newValue);

    default void insert(K key, V value) {
        update(key, null, value);
    }

    default void remove(K key, V value) {
        update(key, value, null);
    }
}