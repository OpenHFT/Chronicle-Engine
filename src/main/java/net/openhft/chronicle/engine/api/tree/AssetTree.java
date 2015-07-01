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
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.map.RawKVSSubscription;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public interface AssetTree extends Closeable {
    @NotNull
    static Class<Subscription> getSubscriptionType(@NotNull RequestContext rc) {
        Class elementType = rc.elementType();
        return elementType == TopologicalEvent.class
                ? (Class) TopologySubscription.class
                : elementType == BytesStore.class
                ? (Class) RawKVSSubscription.class
                : (Class) ObjectKVSSubscription.class;
    }

    @NotNull
    Asset acquireAsset(@NotNull String fullName) throws AssetNotFoundException;

    @Nullable
    Asset getAsset(String fullName);

    Asset root();

    @NotNull
    default <E> Set<E> acquireSet(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("set").type(eClass));
    }

    @NotNull
    default <E> E acquireView(@NotNull RequestContext rc) throws AssetNotFoundException {
        return acquireAsset(rc.fullName()).acquireView(rc);
    }

    @NotNull
    default <S> S acquireService(@NotNull String uri, Class<S> service) throws AssetNotFoundException {
        return acquireView(requestContext(uri).viewType(service));
    }

    @NotNull
    default <K, V> MapView<K, V, V> acquireMap(@NotNull String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("map").type(kClass).type2(vClass));
    }

    @NotNull
    default <E> Publisher<E> acquirePublisher(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("pub").type(eClass));
    }

    @NotNull
    default <E> Reference<E> acquireReference(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("ref").type(eClass));
    }

    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(name).view("topicPub").type(tClass).type2(eClass));
    }

    default <E> void registerSubscriber(@NotNull String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(eClass);
        acquireSubscription(rc)
                .registerSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(@NotNull String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(tClass).type2(eClass);
        ((KVSSubscription) acquireSubscription(rc)).registerTopicSubscriber(rc, subscriber);
    }

    @NotNull
    default Subscription acquireSubscription(@NotNull RequestContext rc) throws AssetNotFoundException {
        Asset asset = acquireAsset(rc.fullName());
        Class<Subscription> subscriptionType = getSubscriptionType(rc);
        rc.viewType(subscriptionType);
        return asset.acquireView(subscriptionType, rc);
    }

    @Nullable
    default Subscription getSubscription(@NotNull RequestContext rc) {
        Class<Subscription> subscriptionType = getSubscriptionType(rc);
        rc.viewType(subscriptionType);
        Asset asset = getAsset(rc.fullName());
        return asset == null ? null : asset.getView(subscriptionType);
    }

    default <E> void unregisterSubscriber(@NotNull String name, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name);
        Subscription subscription = getSubscription(rc);
        if (subscription != null)
            subscription.unregisterSubscriber(subscriber);
    }

    default <T, E> void unregisterTopicSubscriber(@NotNull String name, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscriber.class);
        Subscription subscription = getSubscription(rc);
        if (subscription instanceof KVSSubscription)
            ((KVSSubscription) acquireSubscription(rc)).unregisterTopicSubscriber(subscriber);
    }

    default void enableManagement() {
        ManagementTools.enableManagement(this);
    }

    default void disableManagement(){
        ManagementTools.disableManagement(this);
    }
}
