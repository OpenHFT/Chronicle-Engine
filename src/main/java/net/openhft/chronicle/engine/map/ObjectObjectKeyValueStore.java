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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class ObjectObjectKeyValueStore<K, V> implements KeyValueStore<K, V> {
    @NotNull
    private final BiFunction<K, Bytes, Bytes> keyToBytes;
    @NotNull
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    @NotNull
    private final BiFunction<BytesStore, K, K> bytesToKey;
    @NotNull
    private final BiFunction<BytesStore, V, V> bytesToValue;
    @NotNull
    private final KeyValueStore<BytesStore, BytesStore> kvStore;
    private final Asset asset;

    public ObjectObjectKeyValueStore(@NotNull RequestContext context, Asset asset, Assetted assetted) {
        this.asset = asset;
        @NotNull Class type = context.type();
        keyToBytes = toBytes(type);
        bytesToKey = fromBytes(type);
        Class type2 = context.type2();
        valueToBytes = toBytes(type2);
        bytesToValue = fromBytes(type2);
        kvStore = (KeyValueStore<BytesStore, BytesStore>) assetted;
    }

    private static <T> BiFunction<T, Bytes, Bytes> toBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.appendUtf8((String) t);
        throw new UnsupportedOperationException("todo");
    }

    private <T> BiFunction<BytesStore, T, T> fromBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean put(K key, V value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        return kvStore.put(keyBytes, valueBytes);
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        @Nullable BytesStore retBytes = kvStore.getAndPut(keyBytes, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public boolean remove(K key) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        return kvStore.remove(keyBytes);
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        @Nullable BytesStore retBytes = kvStore.getAndRemove(keyBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Nullable
    @Override
    public V getUsing(K key, Object value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        @Nullable BytesStore retBytes = kvStore.getUsing(keyBytes, b.valueBuffer);
        return retBytes == null ? null : bytesToValue.apply(retBytes, (V) value);
    }

    @Override
    public long longSize() {
        return kvStore.longSize();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        kvStore.keysFor(segment, k -> kConsumer.accept(bytesToKey.apply(k, null)));
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(e.translate(bytesToKey, bytesToValue)));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @NotNull
    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean containsValue(final V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    @Override
    public void close() {
        kvStore.close();
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }
}
