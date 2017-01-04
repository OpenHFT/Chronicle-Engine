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
public class RemovedEvent<K, V> extends AbstractMarshallable implements MapEvent<K, V> {
    private String assetName;
    @Nullable
    private K key;
    @Nullable
    private V oldValue;
    private boolean isReplicationEvent;

    private RemovedEvent(String assetName, @NotNull K key, @Nullable V oldValue, boolean isReplicationEvent) {
        this.assetName = assetName;
        this.key = key;
        this.oldValue = oldValue;
        this.isReplicationEvent = isReplicationEvent;
    }

    @NotNull
    public static <K, V> RemovedEvent<K, V> of(String assetName, @NotNull K key, V value, boolean isReplicationEvent) {
        return new RemovedEvent<>(assetName, key, value, isReplicationEvent);
    }

    @Override
    public String assetName() {
        return assetName;
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new RemovedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(oldValue), isReplicationEvent);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull BiFunction<K, K2, K2> keyFunction, @NotNull BiFunction<V, V2, V2> valueFunction) {
        return new RemovedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(oldValue, null), isReplicationEvent);
    }

    @Nullable
    public K getKey() {
        return key;
    }

    @Nullable
    @Override
    public V oldValue() {
        return oldValue;
    }

    @Nullable
    public V getValue() {
        return null;
    }

    @Override
    public void apply(@NotNull MapEventListener<K, V> listener) {
        listener.remove(assetName, key, oldValue);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(MapEventFields.assetName).text(this, (o, s) -> assetName = s);
        wire.read(MapEventFields.key).object(Object.class, this, (o, x) -> o.key = (K) x);
        wire.read(MapEventFields.oldValue).object(Object.class, this, (o, x) -> o.oldValue = (V) x);
        wire.read(MapEventFields.isReplicationEvent).bool(this, (o, x) -> o.isReplicationEvent = x);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.oldValue).object(oldValue);
        wire.write(MapEventFields.isReplicationEvent).object(isReplicationEvent);
    }
}
