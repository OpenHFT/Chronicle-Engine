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
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.*;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class VanillaStringMarshallableKeyValueStore<V extends Marshallable> implements StringMarshallableKeyValueStore<V> {

    @NotNull
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    @NotNull
    private final BiFunction<BytesStore, V, V> bytesToValue;
    @NotNull
    private final ObjectSubscription<String, V> subscriptions;
    private final SubscriptionKeyValueStore<String, BytesStore> kvStore;
    private final Asset asset;
    private final Class<V> valueType;

    public VanillaStringMarshallableKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset,
                                                  @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectSubscription.class, context), asset, context.valueType(),
                kvStore, context.wireType());
    }

    VanillaStringMarshallableKeyValueStore(@NotNull ObjectSubscription<String, V>
                                                   subscriptions, @NotNull Asset asset, @NotNull Class valueType,
                                           @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore,
                                           @NotNull Function<Bytes, Wire> wireType) {
        this.asset = asset;
        this.valueType = valueType;
        valueToBytes = toBytes(valueType, wireType);
        bytesToValue = fromBytes(valueType, wireType);
        this.kvStore = kvStore;
        ValueReader<BytesStore, V> valueReader = bs -> bytesToValue.apply(bs, null);
        asset.registerView(ValueReader.class, valueReader);
        RawKVSSubscription<String, BytesStore> rawSubscription =
                (RawKVSSubscription<String, BytesStore>) kvStore.subscription(true);
        this.subscriptions = subscriptions;
        rawSubscription.registerDownstream(mpe ->
                subscriptions.notifyEvent(mpe.translate(s -> s, b -> bytesToValue.apply(b, null))));
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(@NotNull Class type, @NotNull Function<Bytes, Wire> wireType) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.appendUtf8((String) t);
        if (Marshallable.class.isAssignableFrom(type))
            return (t, bytes) -> {
                t = acquireInstance(type, t);
                ((Marshallable) t).writeMarshallable(wireType.apply(bytes));
                return bytes;
            };
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    static <T> T acquireInstance(@NotNull Class type, @Nullable T t) {
        if (t == null)
            t = (T) ObjectUtils.newInstance(type);
        return t;
    }

    private <T> BiFunction<BytesStore, T, T> fromBytes(@NotNull Class type, @NotNull Function<Bytes, Wire> wireType) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        if (Marshallable.class.isAssignableFrom(type))
            return (bytes, t) -> {
                if (bytes == null)
                    return null;

                t = acquireInstance(type, t);
                ((Marshallable) t).readMarshallable(wireType.apply(bytes.bytesForRead()));
                return t;
            };
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public ObjectSubscription<String, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public boolean put(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        return kvStore.put(key, valueBytes);
    }

    @Override
    public V getAndPut(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(key, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public boolean remove(String key) {
        return kvStore.remove(key);
    }

    @Override
    public V getAndRemove(String key) {
        BytesStore retBytes = kvStore.getAndRemove(key);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public V getUsing(String key, Object value) {
        Buffers b = BUFFERS.get();
        BytesStore retBytes = kvStore.getUsing(key, b.valueBuffer);
        return retBytes == null ? null : bytesToValue.apply(retBytes, (V) value);
    }

    @Override
    public long longSize() {
        return kvStore.longSize();
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<String> kConsumer) throws InvalidSubscriberException {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<String, V>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(
                InsertedEvent.of(asset.fullName(), e.getKey(), bytesToValue.apply(e.getValue(), null), false)));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<String, V>> entrySetIterator() {
        // todo optimise
        List<Map.Entry<String, V>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, e -> entries.add(new SimpleEntry<>(e.getKey(), e.getValue())));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
        return entries.iterator();
    }

    @NotNull
    @Override
    public Iterator<String> keySetIterator() {
        // todo optimise
        List<String> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                keysFor(i, entries::add);
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
        return entries.iterator();
    }

    @Override
    public void clear() {
        kvStore.clear();
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

    @NotNull
    @Override
    public Class<String> keyType() {
        return String.class;
    }

    @Override
    public Class<V> valueType() {
        return valueType;
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }
}
