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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.ISubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 29/05/15.
 */
public interface KVSSubscription<K, V> extends SubscriptionCollection<MapEvent<K, V>>, ISubscriber, EventConsumer<K, V> {

    /**
     * Add a Subscription for the keys changed on this Map
     *
     * @param subscriber to add
     * @param filter     a list of filter operations
     */
    void registerKeySubscriber(@NotNull RequestContext rc,
                               @NotNull Subscriber<K> subscriber,
                               @NotNull Filter<K> filter);

    @Deprecated
    default void registerKeySubscriber(@NotNull RequestContext rc,
                                       @NotNull Subscriber<K> subscriber) {
        registerKeySubscriber(rc, subscriber, Filter.empty());
    }

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

    default boolean hasValueSubscribers() {
        return hasSubscribers();
    }
}
