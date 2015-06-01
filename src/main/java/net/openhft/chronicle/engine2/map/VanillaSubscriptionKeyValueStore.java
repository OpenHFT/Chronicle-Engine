package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.pubsub.SimpleSubscription;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V> {
    final SubscriptionKVSCollection<K, V> subscriptions = new VanillaSubscriptionKVSCollection<>(this);
    private final Asset asset;

    public VanillaSubscriptionKeyValueStore(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this(asset, (KeyValueStore<K, MV, V>) assetted.get());
    }

    VanillaSubscriptionKeyValueStore(Asset asset, KeyValueStore<K, MV, V> item) {
        super(item);
        this.asset = asset;
    }

    @Override
    public SubscriptionKVSCollection<K, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);
        subscriptions.notifyUpdate(key, oldValue, value);
        publishValueToChild(key, value);
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null) {
            subscriptions.notifyRemoval(key, oldValue);
            publishValueToChild(key, null);
        }
        return oldValue;
    }

    private void publishValueToChild(K key, V value) {
        Optional.of(key)
                .filter(k -> k instanceof CharSequence)
                .map(Object::toString)
                .map(asset::getChild)
                .map(c -> c.getView(SimpleSubscription.class))
                .ifPresent(v -> v.notifyMessage(value));
    }
}
