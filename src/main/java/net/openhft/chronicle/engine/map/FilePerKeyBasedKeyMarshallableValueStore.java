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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.bytes.NativeBytes.nativeBytes;

public class FilePerKeyBasedKeyMarshallableValueStore<K, V extends Marshallable>
        implements KeyValueStore<K, V> {
    @NotNull
    private static final ThreadLocal<Wire> threadLocalValueWire =
            ThreadLocal.withInitial(() -> new TextWire(nativeBytes()));
    private final FilePerKeyValueStore kvStore;
    private final Function<K, String> keyToString;
    private final Function<String, K> stringToKey;
    private final Supplier<V> createValue;

    public FilePerKeyBasedKeyMarshallableValueStore(
            FilePerKeyValueStore kvStore, Function<K, String> keyToString,
            Function<String, K> stringToKey, Supplier<V> createValue) {
        this.kvStore = kvStore;
        this.keyToString = keyToString;
        this.stringToKey = stringToKey;
        this.createValue = createValue;
    }

    private static Wire valueWire() {
        Wire valueWire = threadLocalValueWire.get();
        valueWire.clear();
        return valueWire;
    }

    @Nullable
    private V bytesToValue(@Nullable BytesStore oldValue) {
        V ret;
        if (oldValue != null) {
            V using = createValue.get();
            using.readMarshallable(new TextWire(oldValue.bytesForRead()));
            ret = using;
        } else {
            ret = null;
        }
        return ret;
    }

    @Override
    public boolean put(K key, V value) {
        Wire valueWire = valueWire();
        value.writeMarshallable(valueWire);
        return kvStore.put(keyToString.apply(key), valueWire.bytes());
    }

    @Nullable
    @Override
    public V getAndPut(K key, @NotNull V value) {
        Wire valueWire = valueWire();
        value.writeMarshallable(valueWire);
        BytesStore oldValue = kvStore.getAndPut(keyToString.apply(key), valueWire.bytes());
        return bytesToValue(oldValue);
    }

    @Override
    public boolean remove(K key) {
        return kvStore.remove(keyToString.apply(key));
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        BytesStore oldValue = kvStore.getAndRemove(keyToString.apply(key));
        return bytesToValue(oldValue);
    }

    @Nullable
    @Override
    public V getUsing(K key, @Nullable Object value) {
        Wire valueWire = valueWire();
        kvStore.getUsing(keyToString.apply(key), valueWire.bytes());
        if (value == null)
            value = createValue.get();
        ((ReadMarshallable) value).readMarshallable(valueWire);
        return (V) value;
    }

    @Override
    public long longSize() {
        return kvStore.longSize();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer)
            throws InvalidSubscriberException {
        kvStore.keysFor(segment, key -> kConsumer.accept(stringToKey.apply(key)));
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<K, V>> kvConsumer)
            throws InvalidSubscriberException {
        String assetName = asset().fullName();
        kvStore.entriesFor(segment, event -> kvConsumer.accept(InsertedEvent.of(assetName,
                stringToKey.apply(event.getKey()), bytesToValue(event.getValue()), false)));
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @Override
    public boolean containsValue(V value) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Asset asset() {
        return kvStore.asset();
    }

    @Nullable
    @Override
    public KeyValueStore<K, V> underlying() {
        return null;
    }

    @Override
    public void close() {
        kvStore.close();
    }

    @Override
    public void accept(EngineReplication.ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException();
    }
}
