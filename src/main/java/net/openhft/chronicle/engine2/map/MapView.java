package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public class MapView<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V>, Assetted<KeyValueStore<K, V>>, View {
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;
    private Asset asset;
    private KeyValueStore<K, V> kvStore;

    public MapView(Asset asset, KeyValueStore<K, V> kvStore, String queryString) {
        this.asset = asset;
        this.kvStore = kvStore;
        queryString = queryString.toLowerCase();
        putReturnsNull = queryString.contains("putreturnsnull=true");
        removeReturnsNull = queryString.contains("removereturnsnull=true");
    }

    @Override
    public void asset(Asset asset) {
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore<K, V> underlying) {
        this.kvStore = underlying;
    }

    @Override
    public KeyValueStore<K, V> underlying() {
        return kvStore;
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

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        //noinspection unchecked
        return asset.acquireView(Set.class, Entry.class, "");
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
        throw new UnsupportedOperationException("todo");
    }
}
