package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.map.RawSubscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public interface AssetTree extends Closeable {

    @NotNull
    <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws
            AssetNotFoundException;

    @Nullable
    Asset getAsset(String fullName);

    Asset root();

    @NotNull
    default <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("set").type(eClass);
        //noinspection unchecked
        Asset asset = acquireAsset(rc.viewType(), rc);
        return asset.acquireView(rc);
    }

    @NotNull
    default <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("map").type(kClass).type2(vClass);
        //noinspection unchecked
        Asset asset = acquireAsset(rc.viewType(), rc);
        return asset.acquireView(rc);
    }

    @NotNull
    default <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("pub").type(eClass);
        //noinspection unchecked
        Asset asset = acquireAsset(rc.viewType(), rc);
        return asset.acquireView(rc);
    }

    @NotNull
    default <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("ref").type(eClass);
        //noinspection unchecked
        Asset asset = acquireAsset(rc.viewType(), rc);
        return asset.acquireView(rc);
    }

    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).view("topicPub").type(tClass).type2(eClass);
        //noinspection unchecked
        Asset asset = acquireAsset(rc.viewType(), rc);
        return asset.acquireView(rc);
    }

    default <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(eClass);
        acquireSubscription(rc).registerSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(tClass).type2(eClass);
        acquireSubscription(rc).registerTopicSubscriber(rc, subscriber);
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
        return (Class) (
                rc.elementType() == BytesStore.class
                        ? RawSubscription.class
                        : ObjectSubscription.class);
    }

    default <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).type(eClass);
        Subscription subscription = getSubscription(rc);
        if (subscription != null)
            subscription.unregisterSubscriber(rc, subscriber);
    }

    default <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(name).viewType(Subscriber.class).type(tClass).type2(eClass);
        Subscription subscription = getSubscription(rc);
        if (subscription != null)
            subscription.unregisterTopicSubscriber(rc, subscriber);
    }
}
