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

package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializableUpdater;
import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.VanillaMapView;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class RemoteMapView<K, MV, V> extends VanillaMapView<K, MV, V> {
    public RemoteMapView(@NotNull RequestContext context, Asset asset, KeyValueStore<K, MV, V> kvStore) {
        this(context.keyType(), context.valueType(), asset, kvStore, context.putReturnsNull() != Boolean.FALSE, context.removeReturnsNull() != Boolean.FALSE);
    }

    public RemoteMapView(Class keyClass, Class valueType, Asset asset, KeyValueStore<K, MV, V> kvStore, boolean putReturnsNull, boolean removeReturnsNull) {
        super(keyClass, valueType, asset, kvStore, putReturnsNull, removeReturnsNull);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.<Object, Boolean>applyTo((SerializableBiFunction) MapFunction.CONTAINS_VALUE, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        this.asyncUpdate((SerializableUpdaterWithArg) MapUpdate.PUT_ALL, m);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        this.asyncUpdate((SerializableUpdaterWithArg) MapUpdate.REPLACE_ALL, function);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Map &&
                this.<Object, Boolean>applyTo((SerializableBiFunction) MapFunction.EQUALS, o);
    }

    @Override
    public int hashCode() {
        return this.<Object, Integer>applyTo((SerializableBiFunction) MapFunction.HASH_CODE, null);
    }

    @Nullable
    @Override
    public V putIfAbsent(@NotNull K key, V value) {
        return (V) this.applyToKey(key, (SerializableBiFunction) MapFunction.PUT_IF_ABSENT, KeyValuePair.of(key, value));
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        return (Boolean) this.applyToKey((K) key, (SerializableBiFunction) MapFunction.REMOVE, KeyValuePair.of(key, value));
    }

    @Override
    public boolean replace(@NotNull K key, @NotNull V oldValue, @NotNull V newValue) {
        return (Boolean) this.applyToKey((K) key, (SerializableBiFunction) MapFunction.REPLACE, KeyValuesTuple.of(key, oldValue, newValue));
    }

    @Nullable
    @Override
    public V replace(@NotNull K key, @NotNull V value) {
        return (V) this.applyToKey((K) key, (SerializableBiFunction) MapFunction.REPLACE, KeyValuePair.of(key, value));
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return (V) this.applyToKey(key, (SerializableBiFunction) MapFunction.COMPUTE_IF_ABSENT, KeyFunctionPair.of(key, mappingFunction));
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return (V) this.applyToKey(key, (SerializableBiFunction) MapFunction.COMPUTE_IF_PRESENT, KeyFunctionPair.of(key, remappingFunction));
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return (V) this.applyToKey(key, (SerializableBiFunction) MapFunction.COMPUTE, KeyFunctionPair.of(key, remappingFunction));
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return (V) this.applyToKey(key, (SerializableBiFunction) MapFunction.MERGE, KeyValueFunctionTuple.of(key, value, remappingFunction));
    }

    // core functionality.
    @Override
    public <A, R> R applyTo(@NotNull SerializableBiFunction<MapView<K, MV, V>, A, R> function, A arg) {
        throw new UnsupportedOperationException("todo CE-95");
    }

    @Override
    public <A> void asyncUpdate(@NotNull SerializableUpdaterWithArg<MapView<K, MV, V>, A> updateFunction, A arg) {
        throw new UnsupportedOperationException("todo CE-95");
    }

    @Override
    public <UA, RA, R> R syncUpdate(@NotNull SerializableUpdaterWithArg<MapView<K, MV, V>, UA> updateFunction, UA ua, @NotNull SerializableBiFunction<MapView<K, MV, V>, RA, R> returnFunction, RA ra) {
        throw new UnsupportedOperationException("todo CE-95");
    }

    // helper functions.
    @Override
    public <R> R applyTo(@NotNull SerializableFunction<MapView<K, MV, V>, R> function) {
        // TODO CE-95 handle this natively.
        return applyTo((x, $) -> function.apply(x), null);
    }

    @Override
    public void asyncUpdate(@NotNull SerializableUpdater<MapView<K, MV, V>> updateFunction) {
        // TODO CE-95 handle this natively.
        asyncUpdate((x, $) -> updateFunction.accept(x), null);
    }

    @Override
    public <R> R syncUpdate(@NotNull SerializableUpdater<MapView<K, MV, V>> updateFunction, @NotNull SerializableFunction<MapView<K, MV, V>, R> returnFunction) {
        // TODO CE-95 handle this natively.
        return syncUpdate((x, $) -> updateFunction.accept(x), null, (x, $) -> returnFunction.apply(x), null);
    }

    @Override
    public <R> R applyToKey(K key, @NotNull SerializableFunction<V, R> function) {
        // TODO CE-95 handle this natively.
        return applyTo((x, k) -> function.apply(x.get(k)), key);
    }

    @Override
    public <T, R> R applyToKey(K key, @NotNull SerializableBiFunction<V, T, R> function, T argument) {
        // TODO CE-95 handle this natively.
        return applyTo((x, kv) -> function.apply(x.get(kv.key), (T) kv.value), KeyValuePair.of(key, argument));
    }

    @Override
    public void asyncUpdateKey(K key, @NotNull SerializableFunction<V, V> updateFunction) {
        // TODO CE-95 handle this natively.
        compute(key, (k, v) -> updateFunction.apply(v));
    }

    @Override
    public <T> void asyncUpdateKey(K key, @NotNull SerializableBiFunction<V, T, V> updateFunction, T argument) {
        // TODO CE-95 handle this natively.
        compute(key, (k, v) -> updateFunction.apply(v, argument));
    }

    @Override
    public <R> R syncUpdateKey(K key, @NotNull SerializableFunction<V, V> updateFunction, @NotNull SerializableFunction<V, R> returnFunction) {
        // TODO CE-95 handle this natively.
        return applyTo((map, kvf) -> returnFunction.apply(map.compute(key, (k, v) -> updateFunction.apply(v))), key);
    }

    @Override
    public <T, RT, R> R syncUpdateKey(K key, @NotNull SerializableBiFunction<V, T, V> updateFunction, @Nullable T updateArgument, @NotNull SerializableBiFunction<V, RT, R> returnFunction, @Nullable RT returnArgument) {
        // TODO CE-95 handle this natively.
        return applyTo((map, kvf) -> returnFunction.apply(map.compute(key, (k, v) -> updateFunction.apply(v, updateArgument)), returnArgument), key);
    }
}
