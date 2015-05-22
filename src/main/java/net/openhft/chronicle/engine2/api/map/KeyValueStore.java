package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Assetted;

import java.util.function.Consumer;

/**
 * Created by peter on 22/05/15.
 */
public interface KeyValueStore<K, V> extends Assetted<KeyValueStore> {

    default void put(K key, V value) {
        getAndPut(key, value);
    }

    V getAndPut(K key, V value);

    default void remove(K key) {
        getAndRemove(key);
    }

    V getAndRemove(K key);

    default V get(K key) {
        return getUsing(key, null);
    }

    V getUsing(K key, V value);

    default boolean containsKey(K key) {
        return get(key) != null;
    }

    default boolean isReadOnly() {
        return false;
    }

    long size();

    default int segments() {
        return 1;
    }

    default int segmentFor(K key) {
        return 0;
    }

    void keysFor(int segment, Consumer<K> kConsumer);

    void entriesFor(int segment, Consumer<Entry<K, V>> kConsumer);

    interface Entry<K, V> {
        K key();

        V value();
    }
}
