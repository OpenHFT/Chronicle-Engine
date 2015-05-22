package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by peter on 22/05/15.
 */
public class AbstractKeyValueStore<K, V> implements KeyValueStore<K, V> {
    KeyValueStore<K, V> kvStore;

    @Override
    public void underlying(KeyValueStore underlying) {
        this.kvStore = underlying;
    }

    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    @Override
    public V getAndPut(K key, V value) {
        return kvStore.getAndPut(key, value);
    }

    @Override
    public V getAndRemove(K key) {
        return kvStore.getAndRemove(key);
    }

    @Override
    public V getUsing(K key, V value) {
        return kvStore.getUsing(key, value);
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return kvStore.entrySetIterator();
    }

    @Override
    public long size() {
        return kvStore.size();
    }

    @Override
    public void keysFor(int segment, Consumer<K> kConsumer) {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<K, V>> kConsumer) {
        kvStore.entriesFor(segment, kConsumer);
    }

    @Override
    public void asset(Asset asset) {
        kvStore.asset(asset);
    }

    @Override
    public Asset asset() {
        return kvStore.asset();
    }
}
