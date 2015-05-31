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
import java.util.stream.Stream;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private final Asset parent;
    private final String name;

    private final Map<Class, Map<String, Function<RequestContext, ViewLayer>>> classifierMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, ViewFactory> factoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private Boolean keyedAsset;

    public VanillaAsset(RequestContext context, Asset asset) {
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
    public <V> void addClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory) {
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
    public boolean isSubAsset() {
        return false;
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
                ViewFactory<V> factory = getFactory(viewType);
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

    private <V> void addView(Class<V> viewType, V v) {
        View view = (View) v;
        if (view.keyedView())
            keyedAsset = true;
        viewMap.put(viewType, view);
        if (v instanceof SubscriptionKeyValueStore)
            topSubscription(((SubscriptionKeyValueStore) v));
    }

    private void topSubscription(SubscriptionKeyValueStore skvStore) {
        viewMap.put(Subscription.class, skvStore.subscription(true));
    }

    @Nullable
    public <I> ViewFactory<I> getFactory(Class<I> iClass) {
        ViewFactory<I> factory = factoryMap.get(iClass);
        if (factory != null)
            return factory;

        if (parent == null) {
            return null;
        }
        return parent.getFactory(iClass);
    }


    @Override
    public <I> ViewFactory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        ViewFactory<I> factory = factoryMap.get(iClass);
        if (factory != null)
            return factory;
        try {
            if (parent == null) {
                throw new AssetNotFoundException("Cannot find or build an factory for " + iClass);
            }
            return parent.acquireFactory(iClass);
        } catch (AssetNotFoundException e) {
            if (iClass != View.class) {
                ViewFactory<ViewFactory> factoryFactory = factoryMap.get(ViewFactory.class);
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
        if (keyedAsset != Boolean.TRUE) {
            int pos = name.indexOf("/");
            if (pos >= 0) {
                String name1 = name.substring(0, pos);
                String name2 = name.substring(pos + 1);
                return getAssetOrANFE(assetClass, context, name1).acquireChild(assetClass, context, name2);
            }
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
        return children.computeIfAbsent(name, keyedAsset != Boolean.TRUE
                ? n -> new VanillaAsset(new RequestContext(context.fullName(), name), this)
                : n -> new VanillaSubAsset(new RequestContext(context.fullName(), name), this));
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
            ViewFactory<Asset> factory = acquireFactory(Asset.class);
            asset = factory.create(requestContext().name(name), this, () -> resource);
        }
        children.put(name, asset);
        return asset;
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, ViewFactory<I> factory) {
        factoryMap.put(iClass, factory);
    }

    @Override
    public String toString() {
        return fullName();
    }

    public void enableTranslatingValuesToBytesStore() {
        addClassifier(MapView.class, "string key maps", rc ->
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
