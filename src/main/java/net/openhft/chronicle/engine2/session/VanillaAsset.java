package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.*;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static net.openhft.chronicle.core.util.StringUtils.split2;
import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private final Asset parent;
    private final String name;
    private final Assetted item;

    final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, Factory> factoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);

    VanillaAsset(RequestContext<Assetted> context) {
        this.parent = context.parent();
        this.name = context.fullName();
        this.item = context.item();
        if ("".equals(name)) {
            assert parent == null;
        } else {
            assert parent != null;
            assert name != null;
        }
    }

    @Override
    public Assetted item() {
        return item;
    }

    @Override
    public <V> V getView(Class<V> vClass) {
        View view = viewMap.get(vClass);
        if (view == null && vClass.isInstance(item)) return (V) item;
        return (V) view;
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
        V view = getView(vClass);
        if (view != null) {
            return view;
        }
        Factory factory = acquireFactory(vClass);
        View view2 = acquireView(vClass, v -> (View) factory.create(requestContext(this).type(v).queryString(queryString)));
        return (V) view2;
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, String queryString) {
        V v = getView(vClass);
        if (v != null)
            return v;
        if (vClass == Set.class) {
            if (class1 == Map.Entry.class) {
                KeyValueStore keyValueStore = acquireView(KeyValueStore.class, class1, queryString);
                return (V) acquireView(EntrySetView.class, aClass ->
                        acquireFactory(EntrySetView.class)
                                .create(requestContext(VanillaAsset.this).queryString(queryString).item(keyValueStore)));
            }
        }
        if (vClass == KeyValueStore.class) {
            KeyValueStore kvStore = (KeyValueStore) viewMap.get(KeyValueStore.class);
            if (kvStore == null) {
                if (item instanceof KeyValueStore)
                    return (V) item;
                else
                    throw new AssetNotFoundException("type: " + vClass);
            }
            return (V) kvStore;
        }
        if (vClass == SubscriptionKeyValueStore.class || vClass == Subscription.class) {
            View view = acquireView(SubscriptionKeyValueStore.class, null, null, queryString);
            return (V) view;
        }
        throw new UnsupportedOperationException("todo " + vClass + " type: " + class1);
    }

    <V> V acquireView(Class viewClass, Function<Class, View> builder) {
        synchronized (viewMap) {
            View view = viewMap.get(viewClass);
            if (view == null)
                viewMap.put(viewClass, view = builder.apply(viewClass));
            return (V) view;
        }
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
        V v = getView(vClass);
        if (v != null)
            return v;
        if (item instanceof KeyValueStore) {
            if ((vClass == Map.class || vClass == ConcurrentMap.class)) {
                return (V) acquireView(MapView.class, class1, class2, queryString);
            }
            if (vClass == MapView.class) {
                View view = acquireView(MapView.class, aClass -> {
                    KeyValueStore kvStore;
                    if (class1 == String.class) {
                        if (class2 == BytesStore.class) {
                            kvStore = acquireView(StringBytesStoreKeyValueStore.class, class1, class2, queryString);
                        } else if (Marshallable.class.isAssignableFrom(class2)) {
                            kvStore = acquireView(StringMarshallableKeyValueStore.class, class1, class2, queryString);
                        } else if (class2 == String.class && getFactory(StringStringKeyValueStore.class) != null) {
                            kvStore = acquireView(StringStringKeyValueStore.class, class1, class2, queryString);
                        } else {
                            kvStore = (KeyValueStore) item;
                        }
                    } else {
                        kvStore = (KeyValueStore) item;
                    }
                    return acquireFactory(MapView.class)
                            .create(requestContext(VanillaAsset.this).type(class1).type2(class2).queryString(queryString).item(kvStore));
                });
                return (V) view;
            }
            if (vClass == StringMarshallableKeyValueStore.class) {
                View view = acquireView(StringMarshallableKeyValueStore.class, aClass -> {
                    KeyValueStore kvStore = acquireView(StringBytesStoreKeyValueStore.class, String.class, BytesStore.class, queryString);
                    StringMarshallableKeyValueStore smkvStore = acquireFactory(StringMarshallableKeyValueStore.class)
                            .create(requestContext(VanillaAsset.this)
                                    .type(class1).type2(class2)
                                    .queryString(queryString)
                                    .wireType((Function<Bytes, Wire>) TextWire::new)
                                    .item(kvStore));
                    topSubscription(smkvStore);
                    return smkvStore;
                });
                return (V) view;
            }
            if (vClass == StringStringKeyValueStore.class) {
                View view = acquireView(StringStringKeyValueStore.class, aClass -> {
                    SubscriptionKeyValueStore kvStore = acquireView(SubscriptionKeyValueStore.class, String.class, BytesStore.class, queryString);
                    StringStringKeyValueStore sskvStore = acquireFactory(StringStringKeyValueStore.class)
                            .create(requestContext(VanillaAsset.this).queryString(queryString).item(kvStore));
                    topSubscription(sskvStore);
                    return sskvStore;
                });
                return (V) view;
            }
            if (vClass == SubscriptionKeyValueStore.class || vClass == Subscription.class) {
                View view = acquireView(SubscriptionKeyValueStore.class, aClass -> {
                    KeyValueStore kvStore = acquireView(KeyValueStore.class, class1, class2, queryString);
                    SubscriptionKeyValueStore skvStore = acquireFactory(SubscriptionKeyValueStore.class)
                            .create(requestContext(VanillaAsset.this).queryString(queryString).item(kvStore));
                    topSubscription(skvStore);
                    return skvStore;
                });
                return (V) view;
            }
            if (vClass == TopicPublisher.class) {
                View view = acquireView(TopicPublisher.class, aClass -> {
                    SubscriptionKeyValueStore subscription = acquireView(SubscriptionKeyValueStore.class, class1, class2, queryString);
                    return acquireFactory(TopicPublisher.class)
                            .create(requestContext(VanillaAsset.this).queryString(queryString).item(subscription));
                });
                return (V) view;
            }
        }
        throw new UnsupportedOperationException("todo " + vClass + " type: " + class1 + " type2: " + class2);
    }

    private void topSubscription(SubscriptionKeyValueStore skvStore) {
        MapView view = getView(MapView.class);
        if (view != null) {
            if (!skvStore.isUnderlying(view.underlying()))
                throw new AssertionError("Miss wiring");
            view.underlying(skvStore);
        }
        viewMap.put(Subscription.class, skvStore);
        viewMap.put(KeyValueStore.class, skvStore);
    }

    public <I> Factory<I> getFactory(Class<I> iClass) {
        Factory<I> factory = factoryMap.get(iClass);
        if (factory != null)
            return factory;

        if (parent == null) {
            return null;
        }
        return parent.getFactory(iClass);
    }


    @Override
    public <I> Factory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        Factory<I> factory = factoryMap.get(iClass);
        if (factory != null)
            return factory;
        try {
            if (parent == null) {
                throw new AssetNotFoundException("Cannot find or build an factory for " + iClass);
            }
            return parent.acquireFactory(iClass);
        } catch (AssetNotFoundException e) {
            if (iClass != View.class) {
                Factory<Factory> factoryFactory = factoryMap.get(Factory.class);
                if (factoryFactory != null) {
                    factory = factoryFactory.create(requestContext(this).type(iClass));
                    if (factory != null) {
                        factoryMap.put(iClass, factory);
                        return factory;
                    }
                }
            }
            throw e;
        }
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        Subscription sub = acquireView(Subscription.class, eClass, query);
        sub.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        Subscription sub = acquireView(Subscription.class, tClass, eClass, query);
        sub.registerTopicSubscriber(tClass, eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        Subscription sub = getView(Subscription.class);
        if (sub != null)
            sub.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        Subscription sub = getView(Subscription.class);
        if (sub != null)
            sub.unregisterTopicSubscriber(tClass, eClass, subscriber, query);
    }

    @Override
    public void close() {
        // throw new UnsupportedOperationException("todo");
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
    public <A> Asset acquireChild(String name, Class<A> assetClass, Class class1, Class class2) throws
            AssetNotFoundException {
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
            Factory<KeyValueStore> kvStoreFactory = acquireFactory(KeyValueStore.class);
            KeyValueStore resource = kvStoreFactory.create(requestContext(this).fullName(nameQuery[0]).queryString(nameQuery[1]).type(class1).type2(class2));
            return add(nameQuery[0], resource);

        } else if ((assetClass == Subscriber.class || assetClass == Publisher.class || assetClass == Reference.class) && item instanceof KeyValueStore) {
            Factory<SubAsset> subAssetFactory = acquireFactory(SubAsset.class);
            SubAsset value = subAssetFactory.create(requestContext(this).fullName(nameQuery[0]).queryString(nameQuery[1]));
            children.put(nameQuery[0], value);
            return value;

        } else if (assetClass == Void.class) {
            Factory<Asset> factory = acquireFactory(Asset.class);
            Asset asset = factory.create(requestContext(this).fullName(nameQuery[0]).queryString(nameQuery[1]));
            children.put(nameQuery[0], asset);
            return asset;

        } else {
            throw new UnsupportedOperationException("todo name: " + name + ", assetClass: " + assetClass
                    + ", class1: " + class1 + ", class2: " + class2);
        }
    }

    @Override
    public Asset getChild(String name) {
        return children.get(name);
    }

    @Nullable
    private Asset getChild0(String name) {
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
        Asset asset;
        if (resource instanceof Asset) {
            asset = (Asset) resource;
        } else {
            Factory<Asset> factory = acquireFactory(Asset.class);
            asset = factory.create(requestContext(this).fullName(name).item(resource));
        }
        children.put(name, asset);
        return asset;
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, Factory<I> factory) {
        factoryMap.put(iClass, factory);
    }

    @Override
    public String toString() {
        return (item == null ? "node" : item.getClass().getSimpleName()) + "@" + fullName();
    }
}
