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

    public MapReference(@NotNull RequestContext context, Asset asset, MapView<String, E> underlying) throws AssetNotFoundException {
        this(context.name(), context.type(), underlying);
    }

    public MapReference(String name, Class<E> type, MapView<String, E> mapView) {
        this.name = name;
        this.eClass = type;
        this.underlyingMap = mapView;
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

        underlyingMap.asset().acquireAsset(name)
                .subscription(true)
                .registerSubscriber(requestContext()
                                .bootstrap(bootstrap)
                                .throttlePeriodMs(throttlePeriodMs)
                                .type(eClass),
                        subscriber, Filter.empty());
    }

    @Override
    public void unregisterSubscriber(Subscriber subscriber) {
        Asset child = underlyingMap.asset().getChild(name);
        if (child != null) {
            SubscriptionCollection subscription = child.subscription(false);
            if (subscription != null)
                subscription.unregisterSubscriber(subscriber);
        }
    }

    @Override
    public int subscriberCount() {
        Asset child = underlyingMap.asset().getChild(name);
        if (child != null) {
            SubscriptionCollection subscription = child.subscription(false);
            if (subscription != null)
                return subscription.subscriberCount();
        }
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
