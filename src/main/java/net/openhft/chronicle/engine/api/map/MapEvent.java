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

    default V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    enum MapEventFields implements WireKey {
        assetName, key, oldValue, value, hasValueChanged, isReplicationEvent
    }
}
