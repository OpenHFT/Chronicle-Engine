package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.TopicSubscriber;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;

import java.util.AbstractMap;
import java.util.Set;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaMapView<K, MV, V> extends AbstractMap<K, V> implements MapView<K, MV, V> {
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;
    private final Class keyClass;
    private final Class valueType;
    private Asset asset;
    private KeyValueStore<K, MV, V> kvStore;

    public VanillaMapView(RequestContext context, Asset asset, Supplier<Assetted> kvStore) {
        this(context.keyType(), context.valueType(), asset, (KeyValueStore<K, MV, V>) kvStore.get(), context.putReturnsNull() != Boolean.FALSE, context.removeReturnsNull() != Boolean.FALSE);
    }

    public VanillaMapView(Class keyClass, Class valueType, Asset asset, KeyValueStore<K, MV, V> kvStore, boolean putReturnsNull, boolean removeReturnsNull) {
        this.keyClass = keyClass;
        this.valueType = valueType;
        this.asset = asset;
        this.kvStore = kvStore;
        this.putReturnsNull = putReturnsNull;
        this.removeReturnsNull = removeReturnsNull;
    }

    @Override
    public boolean containsKey(final Object key) {
        checkKey(key);
        return super.containsKey(key);
    }

    private void checkKey(final Object key) {
        if (key == null)
            throw new NullPointerException("key can not be null");
    }

    private void checkValue(final Object value) {
        if (value == null)
            throw new NullPointerException("value can not be null");
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public KeyValueStore<K, MV, V> underlying() {
        return kvStore;
    }

    @Override
    public V get(Object key) {
        return kvStore.isKeyType(key) ? kvStore.getUsing((K) key, null) : null;
    }

    @Override
    public V put(K key, V value) {
        checkKey(key);
        checkValue(value);
        if (putReturnsNull) {
            kvStore.put(key, value);
            return null;

        } else {
            return kvStore.getAndPut(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        if (!kvStore.isKeyType(key)) {
            return null;
        }
        K key2 = (K) key;
        if (removeReturnsNull) {
            kvStore.remove(key2);
            return null;

        } else {
            return kvStore.getAndRemove(key2);
        }
    }

    @Override
    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, kvStore.size());
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        //noinspection unchecked
        return asset.acquireView(EntrySetView.class, requestContext(asset.fullName()).viewType(EntrySetView.class));
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @Override
    public V putIfAbsent(@NotNull K key, V value) {
        checkKey(key);
        checkValue(value);
        return kvStore.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        checkKey(key);
        checkValue(value);
        return kvStore.isKeyType(key) && kvStore.removeIfEqual((K) key, (V) value);
    }

    @Override
    public boolean replace(@NotNull K key, @NotNull V oldValue, @NotNull V newValue) {
        checkKey(key);
        checkValue(oldValue);
        checkValue(newValue);
        return kvStore.replaceIfEqual(key, oldValue, newValue);
    }

    @Override
    public V replace(@NotNull K key, @NotNull V value) {
        checkKey(key);
        checkValue(value);
        return kvStore.replace(key, value);
    }

    @Override
    public void registerTopicSubscriber(TopicSubscriber<K, V> topicSubscriber) {
        asset.subscription(true).registerTopicSubscriber(RequestContext.requestContext().bootstrap(true).type(keyClass).type2(valueType), topicSubscriber);
    }
}
