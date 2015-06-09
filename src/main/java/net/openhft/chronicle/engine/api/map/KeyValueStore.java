package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.View;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * @param <K>  key type
 * @param <MV> mutable value type
 * @param <V>  immutable value type
 */

public interface KeyValueStore<K, MV, V> extends Assetted<KeyValueStore<K, MV, V>>, View, Closeable {

    /**
     * put an entry
     *
     * @param key   to set
     * @param value to set
     * @return true if it was replaced, false if it was added.
     */
    default boolean put(K key, V value) {
        return getAndPut(key, value) != null;
    }

    V getAndPut(K key, V value);

    /**
     * remove a key
     *
     * @param key to remove
     * @return true if it was removed, false if not.
     */
    default boolean remove(K key) {
        return getAndRemove(key) != null;
    }

    V getAndRemove(K key);

    @Nullable
    default V get(K key) {
        return getUsing(key, null);
    }

    @Nullable
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

    void entriesFor(int segment, SubscriptionConsumer<MapReplicationEvent<K, V>> kvConsumer) throws InvalidSubscriberException;

    Iterator<Map.Entry<K, V>> entrySetIterator();

    void clear();

    @Nullable
    default V replace(K key, V value) {
        if (containsKey(key)) {
            return getAndPut(key, value);
        } else {
            return null;
        }
    }

    default boolean replaceIfEqual(K key, V oldValue, V newValue) {
        if (containsKey(key) && Objects.equals(get(key), oldValue)) {
            put(key, newValue);
            return true;
        } else
            return false;
    }

    default boolean removeIfEqual(K key, V value) {
        if (!isKeyType(key))
            return false;
        if (containsKey(key) && Objects.equals(get(key), value)) {
            remove(key);
            return true;
        } else
            return false;
    }

    default boolean isKeyType(Object key) {
        return true;
    }

    default V putIfAbsent(K key, V value) {
        V value2 = get(key);
        return value2 == null ? getAndPut(key, value) : value2;
    }

    default boolean keyedView() {
        return true;
    }

    interface Entry<K, V> {
        K key();

        V value();
    }
}
