package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.StringMarshallableKeyValueStore;
import net.openhft.chronicle.engine2.api.map.StringStringKeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.map.VanillaStringMarshallableKeyValueStore;
import net.openhft.chronicle.engine2.map.VanillaStringStringKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private final Asset parent;
    private final String name;

    final Map<Class, Map<String, Function<RequestContext, ViewLayer>>> classifierMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, Factory> factoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);

    VanillaAsset(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this(context, asset);
        assert assetted == null;

    }

    VanillaAsset(RequestContext context, Asset asset) {
        this.parent = asset;
        this.name = context.name();

        if ("".equals(name)) {
            assert parent == null;
        } else {
            assert parent != null;
            assert name != null;
        }
    }

    @Override
    public <V> void prependClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory) {
        Map<String, Function<RequestContext, ViewLayer>> stringFunctionMap = classifierMap.computeIfAbsent(assetType, k -> new ConcurrentSkipListMap<>());
        stringFunctionMap.put(name, viewBuilderFactory);
    }

    public ViewLayer classify(Class viewType, RequestContext rc) throws AssetNotFoundException {
        Map<String, Function<RequestContext, ViewLayer>> stringFunctionMap = classifierMap.get(viewType);
        if (stringFunctionMap != null) {
            for (Function<RequestContext, ViewLayer> function : stringFunctionMap.values()) {
                ViewLayer apply = function.apply(rc);
                if (apply != null)
                    return apply;
            }
        }
        if (parent == null)
            throw new AssetNotFoundException("Unable to classify " + viewType + ", rc: " + rc);
        return parent.classify(viewType, rc);
    }

    @Override
    public <V> V getView(Class<V> vClass) {
        View view = viewMap.get(vClass);
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
    public <V> V acquireView(Class<V> viewType, RequestContext rc) {
        synchronized (viewMap) {
            V view = getView(viewType);
            if (view != null) {
                return (V) view;
            }
            ViewLayer classify;
            try {
                classify = classify(viewType, rc);

            } catch (AssetNotFoundException e) {
                Factory<V> factory = getFactory(viewType);
                if (factory == null)
                    throw e;
                view = (V) factory.create(rc, this, null);
                addView(viewType, view);
                return view;
            }
            view = (V) classify.create(rc, this);
            addView(viewType, view);
            return view;
        }
    }

    private <V> void addView(Class<V> viewType, V view) {
        viewMap.put(viewType, (View) view);
        if (view instanceof SubscriptionKeyValueStore)
            topSubscription(((SubscriptionKeyValueStore) view));
    }

    private void topSubscription(SubscriptionKeyValueStore skvStore) {
        System.out.println("topSubscription " + skvStore);
        viewMap.put(Subscription.class, skvStore.subscription(true));
    }

    /*
        public <V> V acquireView(Class<V> vClass, String queryString) {
            V view = getView(vClass);
            if (view != null) {
                return view;
            }
            if (vClass == Subscription.class) {
                if (item instanceof KeyValueStore) {
                    SubscriptionKeyValueStore skvStore = acquireView(SubscriptionKeyValueStore.class, queryString);
                    topSubscription(skvStore);
                    return (V) skvStore.subscription(true);
                }
            }
            Factory factory = acquireFactory(vClass);
            View view2 = acquireView(vClass, v -> (View) factory.create(requestContext(this).type(v).queryString(queryString)));
            return (V) view2;
        }

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
                KeyValueStore kvStore = getView(KeyValueStore.class);
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
            try {
                Factory factory = acquireFactory(vClass);
                return acquireView(vClass, viewType -> (View) factory.create(requestContext(this).type(class1).queryString(queryString)));
            } catch (AssetNotFoundException e) {
                throw new UnsupportedOperationException("todo " + vClass + " type: " + class1);
            }
        }

        <V> V acquireView(Class viewClass, Function<Class<V>, View> builder) {
            synchronized (viewMap) {
                View view = viewMap.get(viewClass);
                if (view == null)
                    viewMap.put(viewClass, view = builder.apply(viewClass));
                return (V) view;
            }
        }

        public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
            V v = getView(vClass);
            if (v != null)
                return v;
            if (item instanceof KeyValueStore) {
                if (vClass == MapView.class) {
                    View view = acquireView(MapView.class, aClass -> {
                        KeyValueStore kvStore;
                        if (class1 == String.class) {
                            if (class2 == BytesStore.class) {
                                kvStore = acquireView(StringBytesStoreKeyValueStore.class, class1, class2, queryString);
                            } else if (BytesMarshallable.class.isAssignableFrom(class2)) {
                                kvStore = new ChronicleMapKeyValueStore(requestContext(VanillaAsset.this).type(class1).type2(class2).queryString(queryString));
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

                if (vClass == StringBytesStoreKeyValueStore.class) {
                    View view = acquireView(SubscriptionKeyValueStore.class, aClass -> {
                        SubscriptionKeyValueStore kvStore = acquireView(SubscriptionKeyValueStore.class, String.class, BytesStore.class, queryString);
                        StringBytesStoreKeyValueStore sskvStore = acquireFactory(StringBytesStoreKeyValueStore.class)
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

        <V> void putView(Class<V> vClass, V v) {
            if (vClass.isInstance(v))
                viewMap.put(vClass, (View) v);
            else
                throw new IllegalArgumentException(v + "is not a " + vClass);
        }

        private void topSubscription(SubscriptionKeyValueStore skvStore) {
            MapView view = getView(MapView.class);
            if (view != null) {
                if (!skvStore.isUnderlying(view.underlying()))
                    throw new AssertionError("Miss wiring");
                view.underlying(skvStore);
            }
            putView(Subscription.class, skvStore.subscription(true));
            putView(KeyValueStore.class, skvStore);
        }
    */
    @Nullable
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
                    factory = factoryFactory.create(requestContext(), this, () -> {
                        throw new UnsupportedOperationException();
                    });
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
    public <I> void registerView(Class<I> viewType, I view) {
        viewMap.put(viewType, (View) view);
    }

    @Override
    public Subscription subscription(boolean createIfAbsent) {
        return createIfAbsent ? acquireView(Subscription.class, requestContext()) : getView(Subscription.class);
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
    public <A> Asset acquireChild(Class<A> assetClass, RequestContext context, String name) throws AssetNotFoundException {
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            return getAssetOrANFE(assetClass, context, name1).acquireChild(assetClass, context, name2);
        }
        return getAssetOrANFE(assetClass, context, name);
    }

    private Asset getAssetOrANFE(Class assetClass, RequestContext context, String name) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null) {
            asset = createAsset(assetClass, context, name);
            if (asset == null)
                throw new AssetNotFoundException(name);
        }
        return asset;
    }

    @Nullable
    protected Asset createAsset(Class assetClass, RequestContext context, String name) {
        if (assetClass == null)
            return null;
        return children.computeIfAbsent(name, n -> new VanillaAsset(new RequestContext(context.fullName(), name), this));
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
            getAssetOrANFE(Void.class, null, name1).add(name2, resource);
        }
        if (children.containsKey(name))
            throw new IllegalStateException(name + " already exists");
        Asset asset;
        if (resource instanceof Asset) {
            asset = (Asset) resource;
        } else {
            Factory<Asset> factory = acquireFactory(Asset.class);
            asset = factory.create(requestContext().name(name), this, () -> resource);
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
        return fullName();
    }

    public void enableTranslatingValuesToBytesStore() {
        prependClassifier(MapView.class, "string key maps", rc ->
                        rc.keyType() == String.class
                                ? rc.valueType() == String.class ? (rc2, asset) -> asset.acquireFactory(MapView.class).create(rc2, asset, () -> asset.acquireView(StringStringKeyValueStore.class, rc2))
                                : Marshallable.class.isAssignableFrom(rc.valueType()) ? (rc2, asset) -> asset.acquireFactory(MapView.class).create(rc2, asset, () -> asset.acquireView(StringMarshallableKeyValueStore.class, rc2))
                                : null
                                : null
        );
        registerFactory(StringMarshallableKeyValueStore.class, VanillaStringMarshallableKeyValueStore::new);
        registerFactory(StringStringKeyValueStore.class, VanillaStringStringKeyValueStore::new);
    }
}
