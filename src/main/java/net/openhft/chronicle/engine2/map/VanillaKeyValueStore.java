package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaKeyValueStore<K, V> implements KeyValueStore<K, V> {
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
    private Asset asset;

    public VanillaKeyValueStore(String name) {
    }

    @Override
    public V getAndPut(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V getAndRemove(K key) {
        return map.remove(key);
    }

    @Override
    public V getUsing(K key, V value) {
        return map.get(key);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void keysFor(int segment, Consumer<K> kConsumer) {
        map.keySet().forEach(kConsumer);
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<K, V>> kConsumer) {
        map.entrySet().forEach(e -> kConsumer.accept(new VanillaEntry<>(e.getKey(), e.getValue())));
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void asset(Asset asset) {
        if (this.asset != null) throw new IllegalStateException();
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore underlying) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueStore underlying() {
        return null;
    }
}
