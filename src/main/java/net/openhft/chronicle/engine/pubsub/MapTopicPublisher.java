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

package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.KVSSubscription;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 23/05/15.
 */
public class MapTopicPublisher<T, M> implements TopicPublisher<T, M>, Assetted<MapView<T, M>> {
    private final Class<T> tClass;
    private final Class<M> mClass;
    private final Asset asset;
    private final MapView<T, M> underlying;

    public MapTopicPublisher(@NotNull RequestContext context, Asset asset, @NotNull MapView<T, M> underlying) throws AssetNotFoundException {
        this(asset, context.type(), context.type2(), underlying);
    }

    MapTopicPublisher(Asset asset, Class<T> tClass, Class<M> mClass, MapView<T, M> underlying) {
        this.asset = asset;
        this.tClass = tClass;
        this.mClass = mClass;
        this.underlying = underlying;
    }

    @Override
    public void publish(@NotNull T topic, @NotNull M message) {
        underlying.set(topic, message);
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public MapView<T, M> underlying() {
        return underlying;
    }

    @Override
    public void registerTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) throws AssetNotFoundException {
        KVSSubscription<T, M> subscription = (KVSSubscription) asset.subscription(true);
        subscription.registerTopicSubscriber(RequestContext.requestContext().bootstrap(true).type(tClass).type2(mClass), topicSubscriber);
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) {
        KVSSubscription<T, M> subscription = (KVSSubscription) asset.subscription(false);
        if (subscription != null)
            subscription.unregisterTopicSubscriber(topicSubscriber);
    }

    @Override
    public Publisher<M> publisher(@NotNull T topic) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull T topic, @NotNull Subscriber<M> subscriber) {
        throw new UnsupportedOperationException("todo");
    }
}
