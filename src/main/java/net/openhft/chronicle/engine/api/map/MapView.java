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

import net.openhft.chronicle.engine.api.KeyedVisitable;
import net.openhft.chronicle.engine.api.Updatable;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.KeyedView;
import net.openhft.chronicle.engine.api.tree.RequestContext.Operation;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Interface for Map views.
 */
public interface MapView<K, V> extends ConcurrentMap<K, V>,
        Assetted<Object>,
        Updatable<MapView<K, V>>,
        KeyedVisitable<K, V>,
        Function<K, V>,
        KeyedView {

    @NotNull
    @Override
    KeySetView<K> keySet();

    @NotNull
    @Override
    EntrySetView<K, Object, V> entrySet();

    /**
     * Obtain a value using a mutable buffer provided.
     *
     * @param key   to lookup.
     * @param using a mutable buffer
     * @return the value.
     */
    V getUsing(K key, Object using);

    /**
     * Add a TopicSubscriber to this Map.
     *
     * @param topicSubscriber to add
     */
    void registerTopicSubscriber(@NotNull TopicSubscriber<K, V> topicSubscriber);

    /**
     * Add a Subscription for the keys changed on this Map
     *
     * @param subscriber to add
     */
    void registerKeySubscriber(@NotNull Subscriber<K> subscriber);

    /**
     * Add a Subscription for the keys changed on this Map
     *
     * @param subscriber to add
     * @param filter     a list of filter operations
     */
    void registerKeySubscriber(@NotNull Subscriber<K> subscriber,
                               @NotNull Filter filter,
                               @NotNull Set<Operation> contextOperations);

    /**
     * Add a Subscription for the MapEvents triggered by changes on this Map.
     *
     * @param subscriber the subscriber to the subscription
     */
    void registerSubscriber(@NotNull Subscriber<MapEvent<K, V>> subscriber);

    /**
     * Add a Subscription for the MapEvents triggered by changes on this Map.
     *
     * @param subscriber the subscriber to the subscription
     */
    void registerSubscriber(@NotNull Subscriber<MapEvent<K, V>> subscriber,
                            @NotNull Filter<MapEvent<K, V>> filter,
                            @NotNull Set<Operation> contextOperations);

    /**
     * Obtain a reference the value for a key
     *
     * @param key to bind the reference to
     * @return a reference object.
     */
    Reference<V> referenceFor(K key);

    /**
     * @return the type of the keys
     */
    Class<K> keyType();

    /**
     * @return the type of the values.
     */
    Class<V> valueType();

    @Nullable
    @Override
    default V apply(K k) {
        return get(k);
    }

    default int size() {
        return (int) Math.min(Integer.MAX_VALUE, longSize());
    }

    /**
     * @return the size as a long value.
     */
    long longSize();

    /**
     * Explicitly get the old value before putting a new one.
     *
     * @param key   to lookup
     * @param value to set
     * @return the old value or null if absent
     */
    V getAndPut(K key, V value);

    /**
     * Explicitly get the old value before removing.
     *
     * @param key to remove
     * @return the old value or null if absent.
     */
    V getAndRemove(K key);
}
