package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.core.util.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.engine.utils.StringUtils.split2;

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
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], Set.class).acquireView(Set.class, eClass, parts[1]);
    }

    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], ConcurrentMap.class).acquireView(ConcurrentMap.class, kClass, vClass, parts[1]);
    }

    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], Publisher.class).acquireView(Publisher.class, eClass, parts[1]);
    }

    default <E> TopicPublisher<E> acquireTopicPublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        //noinspection unchecked
        return acquireAsset(parts[0], TopicPublisher.class).acquireView(TopicPublisher.class, eClass, parts[1]);
    }

    default <E> void register(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        acquireAsset(parts[0], String.class)
                .registerSubscriber(eClass, subscriber, parts[1]);
    }

    default <E> void unregister(String name, Class<E> eClass, Subscriber<E> subscriber) {
        String[] parts = split2(name, '?');
        Asset asset = getAsset(parts[0]);
        if (asset != null) {
            asset.unregisterSubscriber(eClass, subscriber, "");
        }
    }

    default <E> void register(String name, Class<E> eClass, TopicSubscriber<E> subscriber) throws AssetNotFoundException {
        String[] parts = split2(name, '?');
        acquireAsset(parts[0], null).registerSubscriber(eClass, subscriber, parts[1]);
    }

    default <E> void unregister(String name, Class<E> eClass, TopicSubscriber<E> subscriber) {
        String[] parts = split2(name, '?');
        Asset asset = getAsset(parts[0]);
        if (asset != null) {
            asset.unregisterSubscriber(eClass, subscriber, parts[1]);
        }
    }
}
