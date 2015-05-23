package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine.utils.StringUtils.split2;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    private final Asset parent;
    private final String name;
    private final Assetted item;
    private final Map<Class, View> viewMap = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<Class, Interceptor> interceptorMap = Collections.synchronizedMap(new LinkedHashMap<>());
    private Subscription subscription;

    VanillaAsset(Asset parent, String name, Assetted item) {
        this.parent = parent;
        this.name = name;
        this.item = item;

        if (item instanceof Subscription) {
            subscription = (Subscription) item;
        } else if (item instanceof KeyValueStore) {
            SubscriptionKeyValueStoreSupplier supplier = parent.acquireInterceptor(SubscriptionKeyValueStoreSupplier.class);
            SubscriptionKeyValueStore skvStore = supplier.get();
            skvStore.underlying((KeyValueStore) item);
            subscription = skvStore;
        } else if (item != null) {
            throw new UnsupportedOperationException("todo " + item);
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
        if (vClass == Set.class) {
            if (class1 == Map.Entry.class) {
                return (V) viewMap.computeIfAbsent(EntrySetView.class, aClass ->
                        acquireInterceptor(EntrySetViewFactory.class)
                                .create(VanillaAsset.this, (KeyValueStore) subscription, queryString));
            }
        }
        if (vClass == TopicPublisher.class) {
            return (V) viewMap.computeIfAbsent(TopicPublisher.class, aClass ->
                    acquireInterceptor(TopicPublisherFactory.class)
                            .create(VanillaAsset.this, (KeyValueStore) subscription, queryString));
        }
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
        if (vClass == Map.class || vClass == ConcurrentMap.class) {
            return (V) viewMap.computeIfAbsent(MapView.class, aClass ->
                    acquireInterceptor(MapViewFactory.class)
                            .create(VanillaAsset.this, (KeyValueStore) subscription, queryString));
        }
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <I extends Interceptor> I acquireInterceptor(Class<I> iClass) throws AssetNotFoundException {
        I interceptor = (I) interceptorMap.get(iClass);
        if (interceptor != null)
            return (I) interceptor;
        try {
            if (parent == null) {
                throw new AssetNotFoundException("Cannot find an interceptor for " + iClass);
            }
            return parent.acquireInterceptor(iClass);
        } catch (AssetNotFoundException e) {
            if (iClass != InterceptorFactory.class) {
                InterceptorFactory interceptorFactory = (InterceptorFactory) interceptorMap.get(InterceptorFactory.class);
                if (interceptorFactory != null) {
                    interceptor = interceptorFactory.create(iClass);
                    if (interceptor != null) {
                        interceptorMap.put(iClass, interceptor);
                        return interceptor;
                    }
                }
            }
            throw e;
        }
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscription.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscription.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscription.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscription.unregisterSubscriber(eClass, subscriber, query);
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
    public <A> Asset acquireChild(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            return getAssetOrANFE(name1, assetClass, class1, class2).acquireChild(name2, assetClass, class1, class2);
        }
        return getAssetOrANFE(name, assetClass, class1, class2);
    }

    private <A> Asset getAssetOrANFE(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null) {
            asset = createAsset(name, assetClass, class1, class2);
            if (asset == null)
                throw new AssetNotFoundException(name);
        }
        return asset;
    }

    @Nullable
    protected <A> Asset createAsset(String name, Class<A> assetClass, Class class1, Class class2) {
        if (assetClass == null)
            return null;
        String[] nameQuery = split2(name, '?');
        if (assetClass == Map.class || assetClass == ConcurrentMap.class) {
            KeyValueStoreFactory kvStoreFactory = acquireInterceptor(KeyValueStoreFactory.class);
            return add(nameQuery[0], kvStoreFactory.create(nameQuery[0], nameQuery[1], class1, class2));

        } else if (assetClass == String.class && subscription instanceof KeyValueStore) {
            SubAssetFactory subAssetFactory = acquireInterceptor(SubAssetFactory.class);
            SubAsset value = subAssetFactory.create(this, nameQuery[0], nameQuery[1]);
            children.put(nameQuery[0], value);
            return value;

        } else if (assetClass == Void.class) {
            AssetFactory assetFactory = acquireInterceptor(AssetFactory.class);
            Asset asset = assetFactory.create(this, nameQuery[0], null);
            children.put(nameQuery[0], asset);
            return asset;

        } else {
            throw new UnsupportedOperationException("todo name:" + name + " asset " + assetClass);
        }
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
            getAssetOrANFE(name1, null, null, null).add(name2, resource);
        }
        if (children.containsKey(name))
            throw new IllegalStateException(name + " already exists");
        AssetFactory assetFactory = acquireInterceptor(AssetFactory.class);
        Asset asset = assetFactory.create(this, name, resource);
        children.put(name, asset);
        return asset;
    }

    public <I extends Interceptor> void registerInterceptor(Class<I> iClass, I interceptor) {
        synchronized (interceptorMap) {
            interceptorMap.put(iClass, interceptor);
        }
    }

    @Override
    public String toString() {
        return (subscription == null ? "node" : subscription.getClass().getSimpleName()) + "@" + fullName();
    }
}
