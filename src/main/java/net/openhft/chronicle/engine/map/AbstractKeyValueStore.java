/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
public class AbstractKeyValueStore<K, V> implements KeyValueStore<K, V> {
    @NotNull
    public final KeyValueStore<K, V> kvStore;
    @NotNull
    final Asset asset;
    private final Class<K> keyType;
    private final Class<V> valueType;

    AbstractKeyValueStore(@NotNull RequestContext rc, @NotNull Asset asset, @NotNull KeyValueStore<K, V> kvStore) {
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
    public V getUsing(K key, Object value) {
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
