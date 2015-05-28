package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.core.util.Closeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public interface Session extends Closeable {
    @NotNull
    default <A> Asset acquireAsset(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        return acquireAsset(requestContext(name).assetType(assetClass).type(class1).type2(class2));
    }

    <A> Asset acquireAsset(RequestContext request) throws AssetNotFoundException;

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
        RequestContext rc = requestContext(name).assetType(Set.class).type(eClass);
        //noinspection unchecked
        return acquireAsset(rc).acquireView(rc);
    }

    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(ConcurrentMap.class).type(kClass).type2(vClass);
        //noinspection unchecked
        return acquireAsset(rc).acquireView(rc);
    }

    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(Publisher.class).type(eClass);
        //noinspection unchecked
        return acquireAsset(rc).acquireView(rc);
    }

    default <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(Reference.class).type(eClass);
        //noinspection unchecked
        return acquireAsset(rc).acquireView(rc);
    }

    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(TopicPublisher.class).type(tClass).type2(eClass);
        //noinspection unchecked
        return acquireAsset(rc).acquireView(rc);
    }

    default <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(Subscriber.class).type(eClass);
        acquireAsset(rc).registerSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(Subscriber.class).type(tClass).type2(eClass);
        getAssetOrANFE(rc.fullName()).registerTopicSubscriber(rc, subscriber);
    }

    default <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) {
        RequestContext rc = requestContext(name).assetType(Subscriber.class).type(eClass);
        Asset asset = getAsset(rc.fullName());
        if (asset != null) {
            asset.unregisterSubscriber(rc, subscriber);
        }
    }

    default <E> void registerFactory(String name, Class<E> eClass, Factory<E> factory) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).assetType(Subscriber.class).type(eClass);
        getAsset(rc.fullName()).registerFactory(rc.type(), factory);
    }

    default <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) {
        RequestContext rc = requestContext(name).assetType(Subscriber.class).type(tClass).type2(eClass);
        Asset asset = getAsset(rc.fullName());
        if (asset != null) {
            asset.unregisterTopicSubscriber(rc, subscriber);
        }
    }
}
