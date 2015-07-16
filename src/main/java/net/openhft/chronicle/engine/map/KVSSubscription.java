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

/**
 * Created by peter on 29/05/15.
 */
public interface KVSSubscription<K, MV, V> extends Subscription<MapEvent<K, V>>, ISubscriber, EventConsumer<K, V> {

    void registerKeySubscriber(RequestContext rc, Subscriber<K> subscriber);

    void unregisterKeySubscriber(Subscriber<K> subscriber);

    void registerTopicSubscriber(RequestContext rc, TopicSubscriber<K, V> subscriber);

    void unregisterTopicSubscriber(TopicSubscriber subscriber);

    void registerDownstream(EventConsumer<K, V> subscription);

    default boolean keyedView() {
        return true;
    }

    boolean needsPrevious();

    void setKvStore(KeyValueStore<K, MV, V> store);

    void notifyEvent(MapEvent<K, V> changeEvent);

    boolean hasSubscribers();
}
