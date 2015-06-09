package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.*;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private static final String LAST = "{last}";

    private final Asset parent;
    private final String uri;

    private final Map<Class, Map<String, Function<RequestContext, ViewLayer>>> classifierMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, ViewFactory> factoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private Boolean keyedAsset;

    public VanillaAsset(Asset asset, String uri) {
        this.parent = asset;
        this.uri = uri;

        if ("".equals(uri)) {
            assert parent == null;
        } else {
            assert parent != null;
            assert uri != null;
        }
    }

    public void standardStack() {
        viewTypeLayersOn(TopicPublisher.class, LAST + " topic publisher", MapView.class);
        registerFactory(TopicPublisher.class, VanillaTopicPublisher::new);

        viewTypeLayersOn(Reference.class, LAST + "reference", MapView.class);
        registerFactory(Reference.class, VanillaReference::new);

        viewTypeLayersOn(Publisher.class, LAST + "publisher", MapView.class);
        registerFactory(Publisher.class, VanillaReference::new);

        viewTypeLayersOn(EntrySetView.class, LAST + " entrySet", MapView.class);
        registerFactory(EntrySetView.class, VanillaEntrySetView::new);

        viewTypeLayersOn(ValuesCollection.class, LAST + " values", MapView.class);

        viewTypeLayersOn(MapEventSubscriber.class, LAST + " MapEvent subscriber", Subscription.class);
// todo CE-54      registerFactory(MapEventSubscriber.class, VanillaMapEventSubscriber::new);

        viewTypeLayersOn(KeySubscriber.class, LAST + " keySet subscriber", Subscription.class);
// todo CE-54      registerFactory(KeySubscriber.class, VanillaKeySubscriber::new);

        viewTypeLayersOn(EntrySetSubscriber.class, LAST + " entrySet subscriber", Subscription.class);
// todo  CE-54     registerFactory(EntrySetView.class, VanillaEntrySetSubscriber::new);

        viewTypeLayersOn(KeySetView.class, LAST + " keySet", MapView.class);
// todo  CE-54     registerFactory(KeySetView.class, VanillaKeySetView::new);

        viewTypeLayersOn(TopicSubscriber.class, LAST + " key,value topic subscriber", Subscription.class);
// todo   CE-54    registerFactory(TopicSubscriber.class, VanillaTopicSubscriber::new);

        addClassifier(Subscriber.class, LAST + " generic subscriber", rc ->
                        rc.elementType() == MapEvent.class ? (rc2, asset) -> asset.acquireFactory(MapEventSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : rc.elementType() == Map.Entry.class ? (rc2, asset) -> asset.acquireFactory(EntrySetSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : (rc2, asset) -> asset.acquireFactory(KeySubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
        );

        viewTypeLayersOn(MapView.class, LAST + " string key maps", AuthenticatedKeyValueStore.class);
        registerFactory(MapView.class, VanillaMapView::new);
    }

    public void forTesting() {
        standardStack();

        viewTypeLayersOn(AuthenticatedKeyValueStore.class, LAST + " string -> marshallable", KeyValueStore.class);
        registerFactory(AuthenticatedKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);

        viewTypeLayersOn(SubscriptionKeyValueStore.class, LAST + " sub -> foundation", KeyValueStore.class);
        registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);

        registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);

        registerFactory(SubscriptionKVSCollection.class, VanillaSubscriptionKVSCollection::new);
        registerFactory(Subscription.class, VanillaSubscriptionKVSCollection::new);

        addView(SessionProvider.class, new VanillaSessionProvider());
    }

    public void forRemoteAccess() {
        standardStack();

        //addView((ClientWiredStatelessTcpConnectionHub));
        registerFactory(TcpConnectionHub.class, TcpConnectionHub::new);
        registerFactory(AuthenticatedKeyValueStore.class, RemoteAuthenticatedKeyValueStore::new);
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
        return uri;
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

    public <V> void addView(Class<V> viewType, V v) {
        View view = (View) v;
        if (view.keyedView()) {
            assert children.isEmpty();
            keyedAsset = true;
        }
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

        // do nothing
        viewMap.values().stream().filter(v -> v instanceof Closeable).forEach(v -> {
            try {
                ((java.io.Closeable) v).close();
            } catch (IOException e) {
                // do nothing
            }
        });
    }

    @Override
    public Asset parent() {
        return parent;
    }

    final ConcurrentMap<String, Asset> children = new ConcurrentSkipListMap<>();

    @NotNull
    @Override
    public Asset acquireAsset(String uri) throws AssetNotFoundException {
        if (keyedAsset != Boolean.TRUE) {
            int pos = uri.indexOf("/");
            if (pos >= 0) {
                String name1 = uri.substring(0, pos);
                String name2 = uri.substring(pos + 1);
                return getAssetOrANFE(name1).acquireAsset(name2);
            }
        }
        return getAssetOrANFE(uri);
    }

    private Asset getAssetOrANFE(String uri) throws AssetNotFoundException {
        Asset asset = children.get(uri);
        if (asset == null) {
            asset = createAsset(uri);
            if (asset == null)
                throw new AssetNotFoundException(uri);
        }
        return asset;
    }

    @Nullable
    protected Asset createAsset(String uri) {
        return children.computeIfAbsent(uri, keyedAsset != Boolean.TRUE
                ? n -> new VanillaAsset(this, uri)
                : n -> new VanillaSubAsset(this, uri));
    }

    @Override
    public Asset getChild(String name) {
        return children.get(name);
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException("todo");
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
        addClassifier(MapView.class, "{Marshalling} string key maps", rc ->
                        rc.keyType() == String.class
                                ? rc.valueType() == String.class ? (rc2, asset) -> asset.acquireFactory(MapView.class).create(rc2, asset, () -> asset.acquireView(StringStringKeyValueStore.class, rc2))
                                : Marshallable.class.isAssignableFrom(rc.valueType()) ? (rc2, asset) -> asset.acquireFactory(MapView.class).create(rc2, asset, () -> asset.acquireView(StringMarshallableKeyValueStore.class, rc2))
                                : null
                                : null
        );
        viewTypeLayersOn(StringStringKeyValueStore.class, "{Marshalling} string -> string", AuthenticatedKeyValueStore.class);
        viewTypeLayersOn(StringMarshallableKeyValueStore.class, "{Marshalling} string -> marshallable", AuthenticatedKeyValueStore.class);

        registerFactory(StringMarshallableKeyValueStore.class, VanillaStringMarshallableKeyValueStore::new);
        registerFactory(StringStringKeyValueStore.class, VanillaStringStringKeyValueStore::new);
    }
}
