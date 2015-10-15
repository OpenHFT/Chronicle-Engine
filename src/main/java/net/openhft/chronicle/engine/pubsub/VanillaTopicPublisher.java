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
public class VanillaTopicPublisher<T, M> implements TopicPublisher<T, M>, Assetted<MapView<T, M>> {
    private final Class<T> tClass;
    private final Class<M> mClass;
    private final Asset asset;
    private final MapView<T, M> underlying;

    public VanillaTopicPublisher(@NotNull RequestContext context, Asset asset, @NotNull MapView<T, M> underlying) throws AssetNotFoundException {
        this(asset, context.type(), context.type2(), underlying);
    }

    VanillaTopicPublisher(Asset asset, Class<T> tClass, Class<M> mClass, MapView<T, M> underlying) {
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
