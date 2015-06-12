package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

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

    public static void forRemoteAccess() {
        assetTree = new VanillaAssetTree().forRemoteAccess();
    }

    public static void defaultSession(AssetTree assetTree) {
        Chassis.assetTree = assetTree;
    }

    public static AssetTree defaultSession() {
        return assetTree;
    }

    @NotNull
    public static <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquireSet(name, eClass);
    }

    @NotNull
    public static <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        return assetTree.acquireMap(name, kClass, vClass);
    }

    @NotNull
    public static <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquireReference(name, eClass);
    }

    @NotNull
    public static <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        return assetTree.acquirePublisher(name, eClass);
    }

    @NotNull
    public static <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
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

    public static <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        assetTree.registerSubscriber(name, eClass, subscriber);
    }

    public static <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) {
        assetTree.unregisterSubscriber(name, eClass, subscriber);
    }

    public static <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        assetTree.registerTopicSubscriber(name, tClass, eClass, subscriber);
    }

    public static <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) {
        assetTree.unregisterTopicSubscriber(name, tClass, eClass, subscriber);
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
