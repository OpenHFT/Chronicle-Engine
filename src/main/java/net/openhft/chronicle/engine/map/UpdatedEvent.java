/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
public class UpdatedEvent<K, V> extends AbstractMarshallable implements MapEvent<K, V> {
    private String assetName;
    @Nullable
    private K key;
    @Nullable
    private V oldValue;
    @Nullable
    private V value;
    private boolean isReplicationEvent;
    private boolean hasValueChanged;

    private UpdatedEvent(String assetName,
                         @NotNull K key, @Nullable V oldValue, @Nullable V value,
                         boolean isReplicationEvent,
                         boolean hasValueChanged) {
        this.assetName = assetName;
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
        this.isReplicationEvent = isReplicationEvent;
        this.hasValueChanged = hasValueChanged;

    }

    @NotNull
    public static <K, V> UpdatedEvent<K, V> of(String assetName, K key, V oldValue, V value,
                                               boolean isReplicationEvent, boolean hasValueChanged) {
        return new UpdatedEvent<>(assetName, key, oldValue, value, isReplicationEvent, hasValueChanged);
    }

    @Override
    public String assetName() {
        return assetName;
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new UpdatedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(oldValue), valueFunction.apply(value), isReplicationEvent, hasValueChanged);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull BiFunction<K, K2, K2> keyFunction, @NotNull BiFunction<V, V2, V2> valueFunction) {
        return new UpdatedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(oldValue, null), valueFunction.apply(value, null), isReplicationEvent, hasValueChanged);
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
        return value;
    }

    @Override
    public void apply(@NotNull MapEventListener<K, V> listener) {
        listener.update(assetName, key, oldValue, value);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(MapEventFields.assetName).text(this, (o, s) -> assetName = s);
        wire.read(MapEventFields.key).object(Object.class, this, (o, x) -> o.key = x);
        wire.read(MapEventFields.oldValue).object(Object.class, this, (o, x) -> o.oldValue = x);
        wire.read(MapEventFields.value).object(Object.class, this, (o, x) -> o.value = x);
        wire.read(MapEventFields.isReplicationEvent).bool(this, (o, x) -> o.isReplicationEvent = x);
        wire.read(MapEventFields.hasValueChanged).bool(this, (o, x) -> o.hasValueChanged = x);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.oldValue).object(oldValue);
        wire.write(MapEventFields.value).object(value);
        wire.write(MapEventFields.isReplicationEvent).object(isReplicationEvent);
        wire.write(MapEventFields.hasValueChanged).object(hasValueChanged);
    }
}
