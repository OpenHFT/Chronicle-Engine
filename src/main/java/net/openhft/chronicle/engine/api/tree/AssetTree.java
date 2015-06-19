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

package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.map.RawKVSSubscription;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public interface AssetTree extends Closeable {

    @NotNull
    Asset acquireAsset(Class assetClass, RequestContext context) throws
            AssetNotFoundException;

    default Asset acquireAsset(RequestContext context) throws AssetNotFoundException {
        return acquireAsset(context.viewType(), context);
    }

    @Nullable
    Asset getAsset(String fullName);

    Asset root();

    @NotNull
    default <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("set").type(eClass));
    }

    @NotNull
    default <E> E acquireView(RequestContext rc) {
        return acquireAsset(rc).acquireView(rc);
    }

    default <S> S acquireService(String uri, Class<S> service) {
        return acquireView(requestContext(uri).viewType(service));
    }

    @NotNull
    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("map").type(kClass).type2(vClass));
    }

    @NotNull
    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("pub").type(eClass));
    }

    @NotNull
    default <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("ref").type(eClass));
    }

    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("topicPub").type(tClass).type2(eClass));
    }

    default <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(eClass);
        acquireSubscription(rc)
                .registerSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(tClass).type2(eClass);
        ((KVSSubscription) acquireSubscription(rc)).registerTopicSubscriber(rc, subscriber);
    }

    default Subscription acquireSubscription(RequestContext rc) {
        Class<Subscription> subscriptionType = getSubscriptionType(rc);
        rc.viewType(subscriptionType);
        Asset asset = acquireAsset(subscriptionType, rc);
        return asset.acquireView(subscriptionType, rc);
    }

    default Subscription getSubscription(RequestContext rc) {
        Class<Subscription> subscriptionType = getSubscriptionType(rc);
        rc.viewType(subscriptionType);
        Asset asset = getAsset(rc.fullName());
        return asset == null ? null : asset.getView(subscriptionType);
    }

    static Class<Subscription> getSubscriptionType(RequestContext rc) {
        Class elementType = rc.elementType();
        return elementType == TopologicalEvent.class
                ? (Class) TopologySubscription.class
                : elementType == BytesStore.class
                ? (Class) RawKVSSubscription.class
                : (Class) ObjectKVSSubscription.class;
    }

    default <E> void unregisterSubscriber(String name, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name);
        Subscription subscription = getSubscription(rc);
        if (subscription != null)
            subscription.unregisterSubscriber(subscriber);
    }

    default <T, E> void unregisterTopicSubscriber(String name, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscriber.class);
        Subscription subscription = getSubscription(rc);
        if (subscription instanceof KVSSubscription)
            ((KVSSubscription) acquireSubscription(rc)).unregisterTopicSubscriber(subscriber);
    }

    default void enableManagement() {
        ManagementTools.enableManagement(this);
    }
}
