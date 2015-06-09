package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaKeyValueStore<K, MV, V> implements KeyValueStore<K, MV, V> {
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
    private Asset asset;

    public VanillaKeyValueStore(RequestContext context, Asset asset, ThrowingSupplier<Assetted, AssetNotFoundException> assetted) {
        this(asset);
    }

    public VanillaKeyValueStore(Asset asset) {
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
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.keySet(), kConsumer);
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapReplicationEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.entrySet(), e -> kvConsumer.accept(EntryEvent.of(e.getKey(), e.getValue(), 0, System.currentTimeMillis())));
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void clear() {
        try {
            for (int i = 0, segs = segments(); i < segs; i++)
                keysFor(i, (K k) -> map.remove(k));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
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
    public void close() {

    }
}
