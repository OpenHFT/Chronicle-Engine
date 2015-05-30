package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.core.util.Closeable;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public interface Session extends Closeable {

    <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws AssetNotFoundException;

    @Nullable
    Asset getAsset(String fullName);

    Asset add(String fullName, Assetted resource);

    default Asset getAssetOrANFE(String name) throws AssetNotFoundException {
        Asset asset = getAsset(name);
        if (asset == null)
            throw new AssetNotFoundException(name);
        return asset;
    }

    default <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("set").type(eClass);
        //noinspection unchecked
        return acquireAsset(rc.viewType(), rc).acquireView(rc);
    }

    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("map").type(kClass).type2(vClass);
        //noinspection unchecked
        return acquireAsset(rc.viewType(), rc).acquireView(rc);
    }

    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("pub").type(eClass);
        //noinspection unchecked
        return acquireAsset(rc.viewType(), rc).acquireView(rc);
    }

    default <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("ref").type(eClass);
        //noinspection unchecked
        return acquireAsset(rc.viewType(), rc).acquireView(rc);
    }

    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("topicPub").type(tClass).type2(eClass);
        //noinspection unchecked
        return acquireAsset(rc.viewType(), rc).acquireView(rc);
    }

    default <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscription.class).type(eClass);
        Asset asset = acquireAsset(rc.viewType(), rc);
        Subscription subscription = asset.acquireView(Subscription.class, rc);
        subscription.registerSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscription.class).type(tClass).type2(eClass);
        Asset asset = acquireAsset(rc.viewType(), rc);
        Subscription subscription = asset.acquireView(Subscription.class, rc);
        subscription.registerTopicSubscriber(rc, subscriber);
    }

    default <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) {
        RequestContext rc = requestContext(name).viewType(Subscription.class).type(eClass);
        Asset asset = getAsset(rc.fullName());
        if (asset != null) {
            Subscription subscription = asset.subscription(false);
            if (subscription != null)
                subscription.unregisterSubscriber(rc, subscriber);
        }
    }

    default <E> void registerFactory(String name, Class<E> eClass, Factory<E> factory) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscriber.class).type(eClass);
        getAsset(rc.fullName()).registerFactory(rc.type(), factory);
    }

    default <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) {
        RequestContext rc = requestContext(name).viewType(Subscriber.class).type(tClass).type2(eClass);
        Asset asset = getAsset(rc.fullName());
        if (asset != null) {
            Subscription subscription = asset.subscription(false);
            if (subscription != null)
                subscription.unregisterTopicSubscriber(rc, subscriber);
        }
    }
}
