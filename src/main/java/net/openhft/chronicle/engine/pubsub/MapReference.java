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
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

public class MapReference<E> implements Reference<E> {
    private final String name;
    private final Class<E> eClass;
    private final MapView<String, E> underlyingMap;
    private final Asset asset;

    public MapReference(@NotNull RequestContext context, Asset asset, MapView<String, E> underlying) throws AssetNotFoundException {
        this(context.name(), context.type(), asset, underlying);
    }

    public MapReference(String name, Class<E> type, Asset asset, MapView<String, E> mapView) {
        assert asset != null;
        this.name = name;
        this.eClass = type;
        this.asset = asset;
        this.underlyingMap = mapView;
        assert underlyingMap != null;
    }

    @Override
    public long set(E event) {
        underlyingMap.set(name, event);
        return 0;
    }

    @Nullable
    @Override
    public E get() {
        return underlyingMap.get(name);
    }

    @Override
    public void remove() {
        underlyingMap.remove(name);
    }

    @Override
    public void registerSubscriber(boolean bootstrap, int throttlePeriodMs, Subscriber<E> subscriber) throws AssetNotFoundException {

        asset.subscription(true)
                .registerSubscriber(requestContext()
                                .bootstrap(bootstrap)
                                .throttlePeriodMs(throttlePeriodMs)
                                .type(eClass),
                        subscriber, Filter.empty());
    }

    @Override
    public void unregisterSubscriber(Subscriber subscriber) {
        SubscriptionCollection subscription = asset.subscription(false);
        if (subscription != null)
            subscription.unregisterSubscriber(subscriber);
    }

    @Override
    public int subscriberCount() {
        SubscriptionCollection subscription = asset.subscription(false);
        if (subscription != null)
            return subscription.subscriberCount();
        return 0;
    }

    @Override
    public Class getType() {
        return eClass;
    }

    @Override
    public String toString() {
        return "MapReference{" +
                "name='" + name + '\'' +
                ", eClass=" + eClass +
                '}';
    }
}
