package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements ObjectKeyValueStore<K, MV, V>, AuthenticatedKeyValueStore<K, MV, V> {
    private final ObjectKVSSubscription<K, MV, V> subscriptions;

    public VanillaSubscriptionKeyValueStore(RequestContext context, Asset asset, KeyValueStore<K, MV, V> item) {
        super(context, asset, item);
        this.subscriptions = asset.acquireView(ObjectKVSSubscription.class, context);
        subscriptions.setKvStore(this);
    }

    @Override
    public ObjectKVSSubscription<K, MV, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public V replace(K key, V value) {
        V oldValue = kvStore.replace(key, value);
        if (oldValue != null) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue, value));
        }
        return oldValue;
    }

    @Override
    public boolean put(K key, V value) {
        if (subscriptions.needsPrevious()) {
            return getAndPut(key, value) != null;
        }
        boolean replaced = kvStore.put(key, value);
            subscriptions.notifyEvent(replaced
                    ? InsertedEvent.of(asset.fullName(), key, value)
                    : UpdatedEvent.of(asset.fullName(), key, null, value));
        return replaced;

    }

    @Override
    public boolean remove(K key) {
        if (subscriptions.needsPrevious()) {
            return getAndRemove(key) != null;
        }
        if (kvStore.remove(key)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, null));
            return true;
        }
        return false;
    }

    @Override
    public boolean replaceIfEqual(K key, V oldValue, V newValue) {
        if (kvStore.replaceIfEqual(key, oldValue, newValue)) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue, newValue));
            return true;
        }
        return false;
    }

    @Override
    public boolean removeIfEqual(K key, V value) {
        if (kvStore.removeIfEqual(key, value)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, value));
            return true;
        }
        return false;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V ret = kvStore.putIfAbsent(key, value);
        if (ret == null)
            subscriptions.notifyEvent(InsertedEvent.of(asset.fullName(), key, value));
        return ret;
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);

            subscriptions.notifyEvent(oldValue == null
                    ? InsertedEvent.of(asset.fullName(), key, value)
                    : UpdatedEvent.of(asset.fullName(), key, oldValue, value));
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null)
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, oldValue));
        return oldValue;
    }
}
