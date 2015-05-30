package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V> {
    final SubscriptionKVSCollection<K, V> subscriptions = new VanillaSubscriptionKVSCollection<>(this);

    public VanillaSubscriptionKeyValueStore(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this((KeyValueStore<K, MV, V>) assetted.get());
    }

    VanillaSubscriptionKeyValueStore(KeyValueStore<K, MV, V> item) {
        super(item);
    }

    @Override
    public SubscriptionKVSCollection<K, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return new VanillaSubscriptionKeyValueStore<>(View.forSession(kvStore, session, asset));
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);
        subscriptions.notifyUpdate(key, oldValue, value);
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null)
            subscriptions.notifyRemoval(key, oldValue);
        return oldValue;
    }
}
