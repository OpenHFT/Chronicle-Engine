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

package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializableUpdater;
import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;

/**
 * Created by peter on 22/05/15.
 */
public class RemoteMapView<K, MV, V> extends VanillaMapView<K, V> {
    @org.jetbrains.annotations.NotNull
    private final RequestContext context;

    public RemoteMapView(@org.jetbrains.annotations.NotNull @NotNull RequestContext context,
                         @org.jetbrains.annotations.NotNull @NotNull Asset asset,
                         @org.jetbrains.annotations.NotNull @NotNull KeyValueStore<K, V> kvStore) {
        super(context, asset, kvStore);
        this.context = context;
    }

    @Override
    public boolean containsValue(Object value) {
        return convertTo(Boolean.class, this.applyTo((SerializableBiFunction) MapFunction.CONTAINS_VALUE, value));
    }

/* TODO CE-95
   Map serialization not supported yet.
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        this.asyncUpdate((SerializableUpdaterWithArg) MapUpdate.PUT_ALL, m);
    }
*/

    @Override
    public void registerKeySubscriber(@org.jetbrains.annotations.NotNull @NotNull Subscriber<K> subscriber,
                                      @org.jetbrains.annotations.NotNull @NotNull Filter filter,
                                      @org.jetbrains.annotations.NotNull @NotNull Set<RequestContext.Operation> contextOperations) {

        @org.jetbrains.annotations.NotNull final KVSSubscription<K, V> subscription = (KVSSubscription<K, V>) asset.subscription(true);
        @org.jetbrains.annotations.NotNull final RequestContext rc = RequestContext.requestContext().type(keyClass).type2(valueType);
        contextOperations.forEach(e -> e.apply(rc));

        subscription.registerKeySubscriber(rc, subscriber, filter);

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

    @org.jetbrains.annotations.NotNull
    @NotNull
    @Override
    public Reference<V> referenceFor(K key) {
        // TODO CE-101
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int hashCode() {
        return this.<Object, Integer>applyTo((SerializableBiFunction) MapFunction.HASH_CODE, null);
    }

    @Nullable
    @Override
    public V putIfAbsent(@NotNull K key, V value) {
        checkKey(key);
        checkValue(value);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.PUT_IF_ABSENT, KeyValuePair.of(key, value));
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        checkKey(key);
        checkValue(value);
        return (Boolean) this.applyTo((SerializableBiFunction) MapFunction.REMOVE, KeyValuePair.of(key, value));
    }

    @Override
    public boolean replace(@NotNull K key, @NotNull V oldValue, @NotNull V newValue) {
        checkKey(key);
        checkValue(oldValue);
        checkValue(newValue);
        @Nullable Object o = this.applyTo((SerializableBiFunction) MapFunction.REPLACE, KeyValuesTuple.of(key, oldValue, newValue));
        return convertTo(Boolean.class, o);
    }

    @Nullable
    @Override
    public V replace(@NotNull K key, @NotNull V value) {
        checkKey(key);
        checkValue(value);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.REPLACE, KeyValuePair.of(key, value));
    }

    @org.jetbrains.annotations.NotNull
    @NotNull
    @Override
    public V computeIfAbsent(K key, @org.jetbrains.annotations.NotNull Function<? super K, ? extends V> mappingFunction) {
        checkKey(key);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.COMPUTE_IF_ABSENT, KeyFunctionPair.of(key, mappingFunction));
    }

    @org.jetbrains.annotations.NotNull
    @NotNull
    @Override
    public V computeIfPresent(K key, @org.jetbrains.annotations.NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkKey(key);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.COMPUTE_IF_PRESENT, KeyFunctionPair.of(key, remappingFunction));
    }

    @org.jetbrains.annotations.NotNull
    @NotNull
    @Override
    public V compute(K key, @org.jetbrains.annotations.NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkKey(key);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.COMPUTE, KeyFunctionPair.of(key, remappingFunction));
    }

    @org.jetbrains.annotations.NotNull
    @NotNull
    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        checkKey(key);
        checkValue(value);
        return (V) this.applyTo((SerializableBiFunction) MapFunction.MERGE, KeyValueFunctionTuple.of(key, value, remappingFunction));
    }

    // core functionality.
    @Nullable
    @Override
    public <A, R> R applyTo(@NotNull SerializableBiFunction<MapView<K, V>, A, R> function, A arg) {
        @Nullable RemoteKeyValueStore<K, V> store = (RemoteKeyValueStore<K, V>) underlying();
        return store.applyTo((SerializableBiFunction<MapView<K, V>, A, R>) (SerializableBiFunction) function, arg);
    }

    @Override
    public <A> void asyncUpdate(@NotNull SerializableUpdaterWithArg<MapView<K, V>, A> updateFunction, A arg) {
        @org.jetbrains.annotations.NotNull RemoteKeyValueStore<K, V> store = (RemoteKeyValueStore<K, V>) underlying();
        store.asyncUpdate((SerializableUpdaterWithArg) updateFunction, arg);
    }

    @Nullable
    @Override
    public <UA, RA, R> R syncUpdate(@NotNull SerializableUpdaterWithArg<MapView<K, V>, UA>
                                            updateFunction, UA ua, @NotNull
                                    SerializableBiFunction<MapView<K, V>, RA, R> returnFunction, RA ra) {
        @org.jetbrains.annotations.NotNull RemoteKeyValueStore<K, V> store = (RemoteKeyValueStore<K, V>) underlying();
        return store.syncUpdate((SerializableUpdaterWithArg) updateFunction, ua, (SerializableBiFunction) returnFunction, ra);
    }

    // helper functions.
    @Nullable
    @Override
    public <R> R applyTo(@NotNull SerializableFunction<MapView<K, V>, R> function) {
        // TODO CE-95 handle this natively.
        return applyTo((x, $) -> function.apply(x), null);
    }

    @Override
    public void asyncUpdate(@NotNull SerializableUpdater<MapView<K, V>> updateFunction) {
        // TODO CE-95 handle this natively.
        asyncUpdate((x, $) -> updateFunction.accept(x), null);
    }

    @Nullable
    @Override
    public <R> R syncUpdate(@NotNull SerializableUpdater<MapView<K, V>> updateFunction, @NotNull
    SerializableFunction<MapView<K, V>, R> returnFunction) {
        // TODO CE-95 handle this natively.
        return syncUpdate((x, $) -> updateFunction.accept(x), null, (x, $) -> returnFunction.apply(x), null);
    }

    @Nullable
    @Override
    public <R> R applyToKey(K key, @NotNull SerializableFunction<V, R> function) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        return applyTo((x, k) -> function.apply(x.get(k)), key);
    }

    @Nullable
    @Override
    public <T, R> R applyToKey(K key, @NotNull SerializableBiFunction<V, T, R> function, T argument) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        return applyTo((map, kv) -> function.apply(map.get(kv.key), (T) kv.value), KeyValuePair.of(key, argument));
    }

    @Override
    public void asyncUpdateKey(K key, @NotNull SerializableFunction<V,
            V> updateFunction) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        @org.jetbrains.annotations.NotNull SerializableBiFunction<K, V, V> kvvBiFunction = (k, v) -> updateFunction.apply(v);
        compute(key, kvvBiFunction);
    }

    @Override
    public <T> void asyncUpdateKey(K key, @NotNull SerializableBiFunction<V, T, V> updateFunction, T argument) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        @org.jetbrains.annotations.NotNull SerializableBiFunction<K, V, V> kvvBiFunction = (k, v) -> updateFunction.apply(v, argument);
        compute(key, kvvBiFunction);
    }

    @Nullable
    @Override
    public <R> R syncUpdateKey(K key, @NotNull
    SerializableFunction<V, V> updateFunction, @NotNull SerializableFunction<V, R>
                                       returnFunction) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        @org.jetbrains.annotations.NotNull SerializableBiFunction<K, V, V> kvvBiFunction = (k, v) -> updateFunction.apply(v);
        return applyTo((map, kvf) -> returnFunction.apply(map.compute(key, kvvBiFunction)), key);
    }

    @Nullable
    @Override
    public <T, RT, R> R syncUpdateKey(K key, @NotNull SerializableBiFunction<V, T, V>
            updateFunction, @Nullable T updateArgument, @NotNull SerializableBiFunction<V, RT,
            R> returnFunction, @Nullable RT returnArgument) {
        checkKey(key);
        // TODO CE-95 handle this natively.
        @org.jetbrains.annotations.NotNull SerializableBiFunction<K, V, V> kvvBiFunction = (k, v) -> updateFunction.apply(v, updateArgument);
        return applyTo((map, kvf) -> returnFunction.apply(map.compute(key, kvvBiFunction), returnArgument), key);
    }
}
