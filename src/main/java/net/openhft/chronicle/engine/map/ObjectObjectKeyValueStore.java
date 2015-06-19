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
public class ObjectObjectKeyValueStore<K, MV extends V, V> implements KeyValueStore<K, MV, V> {
    @NotNull
    private final BiFunction<K, Bytes, Bytes> keyToBytes;
    @NotNull
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    @NotNull
    private final BiFunction<BytesStore, K, K> bytesToKey;
    @NotNull
    private final BiFunction<BytesStore, V, V> bytesToValue;
    private KeyValueStore<BytesStore, Bytes, BytesStore> kvStore;
    private Asset asset;

    public ObjectObjectKeyValueStore(@NotNull RequestContext context, Asset asset, Assetted assetted) {
        this.asset = asset;
        Class type = context.type();
        keyToBytes = toBytes(type);
        bytesToKey = fromBytes(type);
        Class type2 = context.type2();
        valueToBytes = toBytes(type2);
        bytesToValue = fromBytes(type2);
        kvStore = (KeyValueStore<BytesStore, Bytes, BytesStore>) assetted;
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.append((String) t);
        throw new UnsupportedOperationException("todo");
    }

    private <T> BiFunction<BytesStore, T, T> fromBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(keyBytes, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        BytesStore retBytes = kvStore.getAndRemove(keyBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Nullable
    @Override
    public V getUsing(K key, MV value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        BytesStore retBytes = kvStore.getUsing(keyBytes, b.valueBuffer);
        return retBytes == null ? null : bytesToValue.apply(retBytes, value);
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
    public boolean containsValue(final MV value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void apply(@NotNull final ReplicationEntry entry) {
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
}
