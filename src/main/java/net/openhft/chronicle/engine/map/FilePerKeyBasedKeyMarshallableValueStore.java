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

    private static Wire valueWire() {
        Wire valueWire = threadLocalValueWire.get();
        valueWire.bytes().clear();
        return valueWire;
    }

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

    @Nullable
    @Override
    public V getAndPut(K key, @NotNull V value) {
        Wire valueWire = valueWire();
        value.writeMarshallable(valueWire);
        BytesStore oldValue = kvStore.getAndPut(keyToString.apply(key), valueWire.bytes());
        return bytesToValue(oldValue);
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
                stringToKey.apply(event.getKey()), bytesToValue(event.getValue()))));
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
