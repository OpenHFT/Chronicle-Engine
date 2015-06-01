package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.set.EntrySetView;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractMap;
import java.util.Set;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaMapView<K, MV, V> extends AbstractMap<K, V> implements MapView<K, MV, V> {
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;
    private Asset asset;
    private KeyValueStore<K, MV, V> kvStore;

    public VanillaMapView(RequestContext context, Asset asset, Supplier<Assetted> kvStore) {
        this(asset, (KeyValueStore<K, MV, V>) kvStore.get(), context.putReturnsNull() != Boolean.FALSE, context.removeReturnsNull() != Boolean.FALSE);
    }

    public VanillaMapView(Asset asset, KeyValueStore<K, MV, V> kvStore, boolean putReturnsNull, boolean removeReturnsNull) {
        this.asset = asset;
        this.kvStore = kvStore;
        this.putReturnsNull = putReturnsNull;
        this.removeReturnsNull = removeReturnsNull;
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
        return kvStore.getUsing((K) key, null);
    }

    @Override
    public V put(K key, V value) {
        if (putReturnsNull) {
            kvStore.put(key, value);
            return null;

        } else {
            return kvStore.getAndPut(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        if (removeReturnsNull) {
            kvStore.remove((K) key);
            return null;

        } else {
            return kvStore.getAndRemove((K) key);
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
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean replace(@NotNull K key, @NotNull V oldValue, @NotNull V newValue) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V replace(@NotNull K key, @NotNull V value) {
        return kvStore.replace(key, value);
    }
}
