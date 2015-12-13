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

package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.tree.ChangeEvent;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This is an update on a Map.
 */
public interface MapEvent<K, V> extends Map.Entry<K, V>, ChangeEvent {

    default boolean isReplicationEvent() {
        return false;
    }

    @Nullable
    V oldValue();

    void apply(MapEventListener<K, V> listener);

    @NotNull
    <K2, V2> MapEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction);

    @NotNull
    <K2, V2> MapEvent<K2, V2> translate(BiFunction<K, K2, K2> keyFunction, BiFunction<V, V2, V2> valueFunction);

    enum MapEventFields implements WireKey {
        assetName, key, oldValue, value, hasValueChanged, isReplicationEvent
    }

    default V setValue(V value) {
        throw new UnsupportedOperationException();
    }
}
