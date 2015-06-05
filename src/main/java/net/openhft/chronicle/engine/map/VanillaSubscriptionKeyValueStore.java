package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V>, AuthenticatedKeyValueStore<K, MV, V> {
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
    public V replace(K key, V value) {
        V oldValue = kvStore.replace(key, value);
        if (oldValue != null) {
            try {
                subscriptions.notifyEvent(UpdatedEvent.of(key, oldValue, value, 0, System.currentTimeMillis()));
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
            publishValueToChild(key, value);
        }
        return oldValue;
    }

    @Override
    public boolean put(K key, V value) {
        if (subscriptions.needsPrevious()) {
            return getAndPut(key, value) != null;
        }
        boolean replaced = kvStore.put(key, value);
        try {
            subscriptions.notifyEvent(replaced
                    ? InsertedEvent.of(key, value, 0, System.currentTimeMillis())
                    : UpdatedEvent.of(key, null, value, 0, System.currentTimeMillis()));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
        publishValueToChild(key, value);
        return replaced;

    }

    @Override
    public boolean remove(K key) {
        if (subscriptions.needsPrevious()) {
            return getAndRemove(key) != null;
        }
        if (kvStore.remove(key)) {
            try {
                subscriptions.notifyEvent(RemovedEvent.of(key, null, 0, System.currentTimeMillis()));
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
            publishValueToChild(key, null);
            return true;
        }
        return false;
    }

    @Override
    public boolean replaceIfEqual(K key, V oldValue, V newValue) {
        if (kvStore.replaceIfEqual(key, oldValue, newValue)) {
            try {
                subscriptions.notifyEvent(UpdatedEvent.of(key, oldValue, newValue, 0, System.currentTimeMillis()));
                publishValueToChild(key, newValue);
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean removeIfEqual(K key, V value) {
        if (kvStore.removeIfEqual(key, value)) {
            try {
                subscriptions.notifyEvent(RemovedEvent.of(key, value, 0, System.currentTimeMillis()));
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
            publishValueToChild(key, null);
            return true;
        }
        return false;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V ret = kvStore.putIfAbsent(key, value);
        if (ret == null)
            try {
                subscriptions.notifyEvent(InsertedEvent.of(key, value, 0, System.currentTimeMillis()));
                publishValueToChild(key, value);
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
        return ret;
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);
        try {
            subscriptions.notifyEvent(oldValue == null
                    ? InsertedEvent.of(key, value, 0, System.currentTimeMillis())
                    : UpdatedEvent.of(key, oldValue, value, 0, System.currentTimeMillis()));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
        publishValueToChild(key, value);
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null) {
            try {
                subscriptions.notifyEvent(RemovedEvent.of(key, oldValue, 0, System.currentTimeMillis()));
            } catch (InvalidSubscriberException e) {
                throw new AssertionError(e);
            }
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
