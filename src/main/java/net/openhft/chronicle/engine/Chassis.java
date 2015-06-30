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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public enum Chassis {
    /* no instances */;
    private static volatile AssetTree assetTree;

    static {
        resetChassis();
    }

    public static void resetChassis() {
        assetTree = new VanillaAssetTree().forTesting();
    }

    public static AssetTree defaultSession() {
        return assetTree;
    }

    @NotNull
    public static <E> Set<E> acquireSet(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquireSet(name, eClass);
    }

    @NotNull
    public static <S> S acquireService(@NotNull String uri, Class<S> serviceClass) {
        return assetTree.acquireService(uri, serviceClass);
    }
    @NotNull
    public static <K, V> MapView<K, V, V> acquireMap(@NotNull String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        return assetTree.acquireMap(name, kClass, vClass);
    }

    @NotNull
    public static <E> Reference<E> acquireReference(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquireReference(name, eClass);
    }

    @NotNull
    public static <E> Publisher<E> acquirePublisher(@NotNull String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquirePublisher(name, eClass);
    }

    @NotNull
    public static <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquireTopicPublisher(name, tClass, eClass);
    }

    @NotNull
    public static <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws
            AssetNotFoundException {
        return assetTree.acquireAsset(assetClass, context);
    }

    @NotNull
    public static Asset acquireAsset(@NotNull RequestContext context) throws AssetNotFoundException {
        return assetTree.acquireAsset(context);
    }

    public static <E> void registerSubscriber(@NotNull String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        assetTree.registerSubscriber(name, eClass, subscriber);
    }

    public static <E> void unregisterSubscriber(@NotNull String name, Subscriber<E> subscriber) {
        assetTree.unregisterSubscriber(name, subscriber);
    }

    public static <T, E> void registerTopicSubscriber(@NotNull String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        assetTree.registerTopicSubscriber(name, tClass, eClass, subscriber);
    }

    public static <T, E> void unregisterTopicSubscriber(@NotNull String name, TopicSubscriber<T, E> subscriber) {
        assetTree.unregisterTopicSubscriber(name, subscriber);
    }

    public static <W, U> void addWrappingRule(Class<W> wClass, String description, WrappingViewFactory<W, U> factory, Class<U> uClass) {
        assetTree.root().addWrappingRule(wClass, description, factory, uClass);
    }

    public static <L> void addLeafRule(Class<L> viewType, String description, LeafViewFactory<L> factory) {
        assetTree.root().addLeafRule(viewType, description, factory);
    }

    // TODO can we hide this.
    public static void enableTranslatingValuesToBytesStore() {
        ((VanillaAsset) assetTree.root()).enableTranslatingValuesToBytesStore();
    }

    @Nullable
    public static Asset getAsset(String name) {
        return assetTree.getAsset(name);
    }

    @NotNull
    public static <A> Asset acquireAsset(@NotNull String name, Class<A> assetClass, Class class1, Class class2) {
        return assetTree.acquireAsset(assetClass, RequestContext.requestContext(name).type(class1).type2(class2));
    }

    public static void close() {
        assetTree.close();
    }
}
