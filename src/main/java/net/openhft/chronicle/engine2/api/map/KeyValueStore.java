package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.InvalidSubscriberException;
import net.openhft.chronicle.engine2.api.SubscriptionConsumer;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.map.VanillaEntry;

import java.util.Iterator;
import java.util.Map;

/**
 * @param <K>  key type
 * @param <MV> mutable value type
 * @param <V>  immutable value type
 */

public interface KeyValueStore<K, MV, V> extends Assetted<KeyValueStore<K, MV, V>>, View, Closeable {

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

    V getUsing(K key, MV value);

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

    void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException;

    void entriesFor(int segment, SubscriptionConsumer<Entry<K, V>> kvConsumer) throws InvalidSubscriberException;

    Iterator<Map.Entry<K, V>> entrySetIterator();

    void clear();

    default V replace(K key, V value) {
        if (containsKey(key)) {
            return getAndPut(key, value);
        } else {
            return null;
        }
    }

    interface Entry<K, V> {
        K key();

        V value();

        static <K, V> Entry<K, V> of(K key, V value) {
            return new VanillaEntry<>(key, value);
        }
    }

    default boolean keyedView() {
        return true;
    }
}
