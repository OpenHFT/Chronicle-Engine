package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.map.ChangeEvent;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    public VanillaKeyValueStore(RequestContext context, Asset asset) {
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
    public long longSize() {
        return map.size();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.keySet(), kConsumer);
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<ChangeEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.entrySet(), e -> kvConsumer.accept(InsertedEvent.of(e.getKey(), e.getValue())));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return map.entrySet().iterator();
    }

    @Override
    public Iterator<K> keySetIterator() {
        return map.keySet().iterator();
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

    @Nullable
    @Override
    public KeyValueStore underlying() {
        return null;
    }

    @Override
    public void close() {

    }
}
