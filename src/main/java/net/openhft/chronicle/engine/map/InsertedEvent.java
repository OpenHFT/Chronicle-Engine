/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
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

import java.util.Objects;
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
    public int hashCode() {
        return Objects.hash("inserted", key, value);
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(MapEventFields.assetName).text(this, (o, s) -> assetName = s);
        wire.read(MapEventFields.key)
                .object(Object.class, this, (o, x) -> o.key = x);
        wire.read(MapEventFields.value).object(Object.class, this, (o, x) -> o.value = x);
        wire.read(MapEventFields.isReplicationEvent).bool(this, (o, x) -> o.isReplicationEvent = x);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.value).object(value);
        wire.write(MapEventFields.isReplicationEvent).bool(isReplicationEvent);
    }
}
