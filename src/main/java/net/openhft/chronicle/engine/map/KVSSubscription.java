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

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.ISubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 29/05/15.
 */
public interface KVSSubscription<K, V> extends Subscription<MapEvent<K, V>>, ISubscriber, EventConsumer<K, V> {

    /**
     * Add a Subscription for the keys changed on this Map
     *
     * @param subscriber to add
     * @param filter     a list of filter operations
     */
    void registerKeySubscriber(@NotNull RequestContext rc,
                               @NotNull Subscriber<K> subscriber,
                               @NotNull Filter<K> filter);

    void registerTopicSubscriber(@NotNull RequestContext rc,
                                 @NotNull TopicSubscriber<K, V> subscriber);

    void unregisterTopicSubscriber(@NotNull TopicSubscriber subscriber);

    void registerDownstream(@NotNull EventConsumer<K, V> subscription);

    default boolean keyedView() {
        return true;
    }

    boolean needsPrevious();

    void setKvStore(KeyValueStore<K, V> store);

    void notifyEvent(MapEvent<K, V> changeEvent);

    boolean hasSubscribers();
}
