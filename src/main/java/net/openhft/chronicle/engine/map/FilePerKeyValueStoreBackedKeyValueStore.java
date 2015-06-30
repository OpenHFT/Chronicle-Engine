/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public class FilePerKeyValueStoreBackedKeyValueStore<K, V> implements KeyValueStore<K, V, V> {

    private final FilePerKeyValueStore filePerKeyValueStore;
    private final Function<K, String> keyToString;
    private final Function<V, BytesStore> valueToBytesStore;
    private final Function<BytesStore, V> bytesStoreToValue;

    public FilePerKeyValueStoreBackedKeyValueStore(
            FilePerKeyValueStore filePerKeyValueStore,
            Function<K, String> keyToString,
            Function<V, BytesStore> valueToBytesStore, Function<BytesStore, V> bytesStoreToValue) {
        this.filePerKeyValueStore = filePerKeyValueStore;
        this.keyToString = keyToString;
        this.valueToBytesStore = valueToBytesStore;
        this.bytesStoreToValue = bytesStoreToValue;
    }

    private V nullableToValue(BytesStore outputValue) {
        return outputValue != null ? bytesStoreToValue.apply(outputValue) : null;
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        BytesStore inputValue = valueToBytesStore.apply(value);
        BytesStore outputValue = filePerKeyValueStore.getAndPut(keyToString.apply(key), inputValue);
        return nullableToValue(outputValue);
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        return nullableToValue(filePerKeyValueStore.getAndRemove(keyToString.apply(key)));
    }

    @Nullable
    @Override
    public V getUsing(K key, V value) {
        // TODO using
        return nullableToValue(filePerKeyValueStore.get(keyToString.apply(key)));
    }

    @Override
    public long longSize() {
        return filePerKeyValueStore.longSize();
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer)
            throws InvalidSubscriberException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapEvent<K, V>> kvConsumer)
            throws InvalidSubscriberException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        filePerKeyValueStore.clear();
    }

    @Override
    public boolean containsValue(V value) {
        return filePerKeyValueStore.containsValue(valueToBytesStore.apply(value));
    }

    @Override
    public Asset asset() {
        return filePerKeyValueStore.asset();
    }

    @Nullable
    @Override
    public KeyValueStore<K, V, V> underlying() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        filePerKeyValueStore.close();
    }

    @Override
    public void accept(EngineReplication.ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException();
    }
}
