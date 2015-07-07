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

import net.openhft.chronicle.engine.api.KeyedVisitable;
import net.openhft.chronicle.engine.api.Updatable;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.View;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Interface for Map views.
 */
public interface MapView<K, MV, V> extends ConcurrentMap<K, V>,
        Assetted<KeyValueStore<K, MV, V>>,
        Updatable<MapView<K, MV, V>>,
        KeyedVisitable<K, V>,
        Function<K, V>,
        View {
    default boolean keyedView() {
        return true;
    }

    /**
     * Obtain a value using a mutable buffer privided.
     *
     * @param key   to lookup.
     * @param using a mutable buffer
     * @return the value.
     */
    V getUsing(K key, MV using);

    /**
     * Add a TopicSubscriber to this Map.
     *
     * @param topicSubscriber to add
     */
    void registerTopicSubscriber(TopicSubscriber<K, V> topicSubscriber);

    /**
     * Add a Subscription for the keys changed on this Map
     *
     * @param subscriber to add
     */
    void registerKeySubscriber(Subscriber<K> subscriber);

    /**
     * Add a Subscription for the MapEvents triggered by changes on this Map.
     *
     * @param subscriber
     */
    void registerSubscriber(Subscriber<MapEvent<K, V>> subscriber);

    /**
     * @return the type of the keys
     */
    Class<K> keyType();

    /**
     * @return the type of the values.
     */
    Class<V> valueType();

    @Override
    default V apply(K k) {
        return get(k);
    }
}
