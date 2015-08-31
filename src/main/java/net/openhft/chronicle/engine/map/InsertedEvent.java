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

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class InsertedEvent<K, V> implements MapEvent<K, V> {
    private String assetName;
    @NotNull
    private K key;
    @NotNull
    private V value;

    private InsertedEvent(String assetName, @NotNull K key, @NotNull V value) {
        this.assetName = assetName;
        this.key = key;
        this.value = value;
    }

    @NotNull
    public static <K, V> InsertedEvent<K, V> of(String assetName, K key, V value) {
        return new InsertedEvent<>(assetName, key, value);
    }

    @Override
    public String assetName() {
        return assetName;
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(value));
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull BiFunction<K, K2, K2> keyFunction, @NotNull BiFunction<V, V2, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(value, null));
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
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof InsertedEvent)
                .map(o -> (InsertedEvent<K, V>) o)
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> BytesUtil.equals(key, e.key))
                .filter(e -> BytesUtil.equals(value, e.value))
                .isPresent();
    }

    @NotNull
    @Override
    public String toString() {
        return "InsertedEvent{" +
                "assetName='" + assetName + '\'' +
                ", key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(MapEventFields.assetName).text(this, (o, s) -> assetName = s);
        wire.read(MapEventFields.key).object((Class<K>) Object.class, this, (o, x) -> o.key = x);
        wire.read(MapEventFields.value).object((Class<V>) Object.class, this, (o, x) -> o.value = x);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.value).object(value);
    }
}
