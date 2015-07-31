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
import net.openhft.chronicle.core.ClassLocal;
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

import java.lang.reflect.Constructor;
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
    private static final ClassLocal<Constructor> CONSTRUCTORS = ClassLocal.withInitial(c -> {
        try {
            Constructor con = c.getDeclaredConstructor();
            con.setAccessible(true);
            return con;
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    });
    @NotNull
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    @NotNull
    private final BiFunction<BytesStore, V, V> bytesToValue;
    @NotNull
    private final ObjectKVSSubscription<String, V, V> subscriptions;
    private final SubscriptionKeyValueStore<String, BytesStore> kvStore;
    private final Asset asset;
    private final Class<V> valueType;

    public VanillaStringMarshallableKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset,
                                                  @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectKVSSubscription.class, context), asset, context.valueType(),
                kvStore, context.wireType());
    }

    VanillaStringMarshallableKeyValueStore(@NotNull ObjectKVSSubscription<String, V, V> subscriptions, @NotNull Asset asset, @NotNull Class valueType,
                                           @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore,
                                           @NotNull Function<Bytes, Wire> wireType) {
        this.asset = asset;
        this.valueType = valueType;
        valueToBytes = toBytes(valueType, wireType);
        bytesToValue = fromBytes(valueType, wireType);
        this.kvStore = kvStore;
        ValueReader<BytesStore, V> valueReader = bs -> bytesToValue.apply(bs, null);
        asset.registerView(ValueReader.class, valueReader);
        RawKVSSubscription<String, Bytes, BytesStore> rawSubscription =
                (RawKVSSubscription<String, Bytes, BytesStore>) kvStore.subscription(true);
        this.subscriptions = subscriptions;
        rawSubscription.registerDownstream(mpe ->
                subscriptions.notifyEvent(mpe.translate(s -> s, b -> bytesToValue.apply(b, null))));
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(@NotNull Class type, @NotNull Function<Bytes, Wire> wireType) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.append((String) t);
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
            try {
                t = (T) CONSTRUCTORS.get(type).newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
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
    public ObjectKVSSubscription<String, V, V> subscription(boolean createIfAbsent) {
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
                InsertedEvent.of(asset.fullName(), e.key(), bytesToValue.apply(e.value(), null))));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<String, V>> entrySetIterator() {
        // todo optimise
        List<Map.Entry<String, V>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, e -> entries.add(new SimpleEntry<>(e.key(), e.value())));
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
