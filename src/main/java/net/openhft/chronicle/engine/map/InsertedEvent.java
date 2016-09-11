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

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class InsertedEvent<K, V> extends AbstractMarshallable implements MapEvent<K, V> {
    private boolean isReplicationEvent;
    private String assetName;
    @NotNull
    private K key;
    @Nullable
    private V value;

    private InsertedEvent(String assetName, @NotNull K key, @Nullable V value, boolean isReplicationEvent) {
        this.assetName = assetName;
        this.key = key;
        this.value = value;
        this.isReplicationEvent = isReplicationEvent;
    }

    @NotNull
    public static <K, V> InsertedEvent<K, V> of(String assetName, K key, V value, boolean isReplicationEvent) {
        return new InsertedEvent<>(assetName, key, value, isReplicationEvent);
    }

    @Override
    public String assetName() {
        return assetName;
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(value), isReplicationEvent);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull BiFunction<K, K2, K2> keyFunction, @NotNull BiFunction<V, V2, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(value, null), isReplicationEvent);
    }

    @Nullable
    public K getKey() {
        return key;
    }

    @Nullable
    @Override
    public V oldValue() {
        return null;
    }

    @Nullable
    public V getValue() {
        return value;
    }

    @Override
    public void apply(@NotNull MapEventListener<K, V> listener) {
        listener.insert(assetName, key, value);
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        assetName = wire.read(MapEventFields.assetName).text();
        key = wire.read(MapEventFields.key).object((Class<K>) Object.class);
        value = wire.read(MapEventFields.value).object((Class<V>) Object.class);
        isReplicationEvent = wire.read(MapEventFields.isReplicationEvent).bool();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.value).object(value);
        wire.write(MapEventFields.isReplicationEvent).bool(isReplicationEvent);
    }
}
