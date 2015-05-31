package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaKeyValueStore<K, MV, V> implements KeyValueStore<K, MV, V> {
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
    private Asset asset;

    public VanillaKeyValueStore(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this.asset = asset;
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
    public V getUsing(K key, MV value) {
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
    public void entriesFor(int segment, Consumer<Entry<K, V>> kvConsumer) {
        map.entrySet().forEach(e -> kvConsumer.accept(new VanillaEntry<>(e.getKey(), e.getValue())));
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void clear() {
        for (int i = 0, segs = segments(); i < segs; i++)
            keysFor(i, k -> map.remove(k));
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public KeyValueStore underlying() {
        return null;
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }
}
