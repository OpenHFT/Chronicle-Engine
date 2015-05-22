package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.map.MapView;
import net.openhft.chronicle.engine2.map.SubscriptionKeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    private final Asset parent;
    private final String name;
    private final Assetted item;
    private Subscription subscription;

    VanillaAsset(Asset parent, String name, Assetted item) {
        this.parent = parent;
        this.name = name;
        this.item = item;

        if (item instanceof KeyValueStore) {
            SubscriptionKeyValueStore skvStore = new SubscriptionKeyValueStore();
            skvStore.underlying((KeyValueStore) item);
            subscription = skvStore;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public <V> V acquireView(Class<V> vClass, String queryString) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, String queryString) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
        if (vClass == Map.class || vClass == ConcurrentMap.class) {
            return (V) new MapView(this, (KeyValueStore) subscription, queryString);
        }
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <I extends Interceptor> I acquireInterceptor(Class<I> iClass) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber) {
        subscription.registerSubscriber(eClass, subscriber);
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber) {
        subscription.registerSubscriber(eClass, subscriber);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber) {
        subscription.unregisterSubscriber(eClass, subscriber);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber) {
        subscription.unregisterSubscriber(eClass, subscriber);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset parent() {
        return parent;
    }

    @NotNull
    @Override
    public Stream<Asset> children() {
        return children.values().stream();
    }

    final ConcurrentMap<String, Asset> children = new ConcurrentSkipListMap<>();

    @NotNull
    @Override
    public Asset acquireChild(String name) throws AssetNotFoundException {
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            getAssetOrANFE(name1).acquireChild(name2);
        }
        return getAssetOrANFE(name);
    }

    private Asset getAssetOrANFE(String name) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null)
            throw new AssetNotFoundException(name);
        return asset;
    }

    @Override
    public Asset getChild(String name) {
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            Asset asset = getAsset(name1);
            if (asset == null) {
                return null;
            } else {
                return asset.getChild(name2);
            }
        }
        return getAsset(name);
    }

    @Nullable
    private Asset getAsset(String name) {
        return children.get(name);
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException("todo");
    }

    public Asset add(String name, Assetted resource) {
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            getAssetOrANFE(name1).add(name2, resource);
        }
        if (children.containsKey(name))
            throw new IllegalStateException(name + " already exists");
        VanillaAsset asset = new VanillaAsset(this, name, resource);
        children.put(name, asset);
        return asset;
    }
}
