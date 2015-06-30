/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by peter on 22/05/15.
 */
public class AbstractKeyValueStore<K, MV, V> implements KeyValueStore<K, MV, V> {
    @NotNull
    protected final Asset asset;
    @NotNull
    protected final KeyValueStore<K, MV, V> kvStore;
    protected final Class<K> keyType;
    protected final Class<V> valueType;

    protected AbstractKeyValueStore(@NotNull RequestContext rc, @NotNull Asset asset, @NotNull KeyValueStore<K, MV, V> kvStore) {
        assert asset != null;
        assert kvStore != null;
        keyType = rc.keyType();
        valueType = rc.valueType();
        this.asset = asset;
        this.kvStore = kvStore;
    }

    @NotNull
    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        return kvStore.getAndPut(key, value);
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        return kvStore.getAndRemove(key);
    }

    @Nullable
    @Override
    public V getUsing(K key, MV value) {
        return kvStore.getUsing(key, value);
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return kvStore.entrySetIterator();
    }

    @Override
    public long longSize() {
        return kvStore.longSize();
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, kvConsumer);
    }

    @Override
    public Asset asset() {
        return kvStore.asset();
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @Nullable
    @Override
    public V replace(K key, V value) {
        return kvStore.replace(key, value);
    }

    @Override
    public void close() {
        kvStore.close();
    }

    @Override
    public boolean put(K key, V value) {
        return kvStore.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        return kvStore.remove(key);
    }

    @Nullable
    @Override
    public V get(K key) {
        return kvStore.get(key);
    }

    @Override
    public boolean containsKey(K key) {
        return kvStore.containsKey(key);
    }

    @Override
    public boolean isReadOnly() {
        return kvStore.isReadOnly();
    }

    @Override
    public int segments() {
        return kvStore.segments();
    }

    @Override
    public int segmentFor(K key) {
        return kvStore.segmentFor(key);
    }

    @Override
    public boolean replaceIfEqual(K key, V oldValue, V newValue) {
        return kvStore.replaceIfEqual(key, oldValue, newValue);
    }

    @Override
    public boolean removeIfEqual(K key, V value) {
        return kvStore.removeIfEqual(key, value);
    }

    @Override
    public boolean isKeyType(Object key) {
        return kvStore.isKeyType(key);
    }

    @Nullable
    @Override
    public V putIfAbsent(K key, V value) {
        return kvStore.putIfAbsent(key, value);
    }

    @Override
    public boolean keyedView() {
        return kvStore.keyedView();
    }

    @Override
    public boolean containsValue(final V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Iterator<K> keySetIterator() {
        return kvStore.keySetIterator();
    }

    public Class<K> keyType() {
        return keyType;
    }

    public Class<V> valueType() {
        return valueType;
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }
}
