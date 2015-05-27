package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.core.util.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.core.util.StringUtils.split2;

/**
 * Created by peter on 22/05/15.
 */
public interface Session extends Closeable {
    @NotNull
    <A> Asset acquireAsset(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException;

    @Nullable
    Asset getAsset(String name);

    Asset add(String name, Assetted resource);

    <I> void registerView(Class<I> iClass, I interceptor);

    default Asset getAssetOrANFE(String name) throws AssetNotFoundException {
        Asset asset = getAsset(name);
        if (asset == null)
            throw new AssetNotFoundException(name);
        return asset;
    }

    default <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], Set.class, eClass, null).acquireView(Set.class, eClass, parts[1]);
    }

    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], ConcurrentMap.class, kClass, vClass).acquireView(ConcurrentMap.class, kClass, vClass, parts[1]);
    }

    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], Publisher.class, eClass, null).acquireView(Publisher.class, eClass, parts[1]);
    }

    default <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], Reference.class, eClass, null).acquireView(Reference.class, eClass, parts[1]);
    }

    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], TopicPublisher.class, tClass, eClass).acquireView(TopicPublisher.class, tClass, eClass, parts[1]);
    }

    default <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        acquireAsset(parts[0], Subscriber.class, eClass, null)
                .registerSubscriber(eClass, subscriber, parts[1]);
    }

    default <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        getAssetOrANFE(parts[0]).registerTopicSubscriber(tClass, eClass, subscriber, parts[1]);
    }

    default <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) {
        String[] parts = split2(name, '?');
        Asset asset = getAsset(parts[0]);
        if (asset != null) {
            asset.unregisterSubscriber(eClass, subscriber, "");
        }
    }

    default <E> void registerFactory(String name, Class<E> eClass, Factory<E> factory) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        getAsset(parts[0]).registerFactory(eClass, factory);
    }

    default <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) {
        String[] parts = split2(name, '?');
        Asset asset = getAsset(parts[0]);
        if (asset != null) {
            asset.unregisterTopicSubscriber(tClass, eClass, subscriber, parts[1]);
        }
    }
}
