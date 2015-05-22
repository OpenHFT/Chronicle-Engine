package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.core.util.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public interface Session extends Closeable {
    @NotNull
    <A> Asset acquireAsset(String name, Class<A> assetClass) throws AssetNotFoundException;

    @Nullable
    Asset getAsset(String name);

    Asset add(String name, Assetted resource);

    <I extends Interceptor> void registerInterceptor(Class<I> iClass, I interceptor);

    default <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        //noinspection unchecked
        return acquireAsset(name, Set.class).acquireView(Set.class, eClass, "");
    }

    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        //noinspection unchecked
        return acquireAsset(name, ConcurrentMap.class).acquireView(ConcurrentMap.class, kClass, vClass, "");
    }

    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        //noinspection unchecked
        return acquireAsset(name, Publisher.class).acquireView(Publisher.class, eClass, "");
    }

    default <E> TopicPublisher<E> acquireTopicPublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        //noinspection unchecked
        return acquireAsset(name, TopicPublisher.class).acquireView(TopicPublisher.class, eClass, "");
    }

    default <E> void register(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        acquireAsset(name, null).registerSubscriber(eClass, subscriber);
    }

    default <E> void unregister(String name, Class<E> eClass, Subscriber<E> subscriber) {
        Asset asset = getAsset(name);
        if (asset != null) {
            asset.unregisterSubscriber(eClass, subscriber);
        }
    }

    default <E> void register(String name, Class<E> eClass, TopicSubscriber<E> subscriber) throws AssetNotFoundException {
        acquireAsset(name, null).registerSubscriber(eClass, subscriber);
    }

    default <E> void unregister(String name, Class<E> eClass, TopicSubscriber<E> subscriber) {
        Asset asset = getAsset(name);
        if (asset != null) {
            asset.unregisterSubscriber(eClass, subscriber);
        }
    }
}
