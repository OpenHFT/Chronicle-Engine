package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.engine.set.VanillaKeySetView;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiPredicate;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private static final String LAST = "{last}";
    private static final BiPredicate<RequestContext, Asset> ALWAYS = (rc, asset) -> true;
    final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final ConcurrentMap<String, Asset> children = new ConcurrentSkipListMap<>();
    private final Asset parent;
    @NotNull
    private final String name;
    private final Map<Class, SortedMap<String, WrappingViewRecord>> wrappingViewFactoryMap =
            new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, LeafViewFactory> leafViewFactoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);

    private Boolean keyedAsset;

    public VanillaAsset(Asset asset, @NotNull String name) {
        this.parent = asset;
        this.name = name;

        if ("".equals(name)) {
            assert parent == null;
        } else {
            assert parent != null;
            assert name != null;
        }
    }

    public void standardStack() {
        addWrappingRule(TopicPublisher.class, LAST + " topic publisher", VanillaTopicPublisher::new, MapView.class);

        addWrappingRule(Reference.class, LAST + "reference", VanillaReference::new, MapView.class);
        addWrappingRule(Publisher.class, LAST + "publisher", VanillaReference::new, MapView.class);
        addWrappingRule(EntrySetView.class, LAST + " entrySet", VanillaEntrySetView::new, MapView.class);

//        viewTypeLayersOn(ValuesCollection.class, LAST + " values", MapView.class);

//        viewTypeLayersOn(MapEventSubscriber.class, LAST + " MapEvent subscriber", Subscription.class);

//        addWrappingRule(KeySubscriber.class, LAST + " keySet subscriber", Subscription.class);
// todo CE-54      registerFactory(KeySubscriber.class, VanillaKeySubscriber::new);

//        viewTypeLayersOn(EntrySetSubscriber.class, LAST + " entrySet subscriber", Subscription.class);
// todo  CE-54     registerFactory(EntrySetView.class, VanillaEntrySetSubscriber::new);

        addWrappingRule(KeySetView.class, LAST + " keySet", VanillaKeySetView::new, MapView.class);

//        viewTypeLayersOn(TopicSubscriber.class, LAST + " key,value topic subscriber", Subscription.class);
// todo   CE-54    registerFactory(TopicSubscriber.class, VanillaTopicSubscriber::new);

        addWrappingRule(MapView.class, LAST + " string key maps", VanillaMapView::new, ObjectKeyValueStore.class);
    }

    public void forTesting() {
        standardStack();

        addWrappingRule(ObjectKeyValueStore.class, LAST + " authenticated",
                VanillaSubscriptionKeyValueStore::new, KeyValueStore.class);

        addWrappingRule(AuthenticatedKeyValueStore.class, LAST + " authenticated",
                VanillaSubscriptionKeyValueStore::new, KeyValueStore.class);

        addWrappingRule(SubscriptionKeyValueStore.class, LAST + " sub -> foundation",
                VanillaSubscriptionKeyValueStore::new, KeyValueStore.class);

        addLeafRule(KeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);

        addLeafRule(ObjectSubscription.class, LAST + " vanilla",
                VanillaSubscriptionKVSCollection::new);

        addView(SessionProvider.class, new VanillaSessionProvider());
    }

    public void forRemoteAccess() {
        standardStack();

        addLeafRule(TcpConnectionHub.class, LAST + " hub", TcpConnectionHub::new);
        addWrappingRule(ObjectKeyValueStore.class, LAST + " remote AKVS",
                RemoteKeyValueStore::new, TcpConnectionHub.class);
    }

    public void enableTranslatingValuesToBytesStore() {
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,string map",
                (rc, asset) -> rc.keyType() == String.class && rc.valueType() == String.class,
                VanillaStringStringKeyValueStore::new, AuthenticatedKeyValueStore.class);
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,marshallable map",
                (rc, asset) -> rc.keyType() == String.class && Marshallable.class.isAssignableFrom(rc.valueType()),
                VanillaStringMarshallableKeyValueStore::new, AuthenticatedKeyValueStore.class);

        addLeafRule(RawSubscription.class, LAST + " vanilla",
                VanillaSubscriptionKVSCollection::new);
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        SortedMap<String, WrappingViewRecord> smap = wrappingViewFactoryMap.computeIfAbsent(iClass, k -> new ConcurrentSkipListMap<>());
        smap.put(description, new WrappingViewRecord(predicate, factory, underlyingType));
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        addWrappingRule(iClass, description, ALWAYS, factory, underlyingType);
    }

    @Override
    public <L> void addLeafRule(Class<L> iClass, String description, LeafViewFactory<L> factory) {
        leafViewFactoryMap.put(iClass, factory);
    }

    @Override
    public <I, U> I createWrappingView(Class viewType, RequestContext rc, Asset asset, U underling) throws AssetNotFoundException {
        SortedMap<String, WrappingViewRecord> smap = wrappingViewFactoryMap.get(viewType);
        if (smap != null)
            for (WrappingViewRecord wvRecord : smap.values()) {
                if (wvRecord.predicate.test(rc, asset)) {
                    if (underling == null)
                        underling = (U) asset.acquireView(wvRecord.underlyingType, rc);
                    return (I) wvRecord.factory.create(rc, asset, underling);
                }
            }
        if (parent == null)
            return null;
        return parent.createWrappingView(viewType, rc, asset, underling);
    }

    @Override
    public <I> I createLeafView(Class viewType, RequestContext rc, Asset asset) throws AssetNotFoundException {
        LeafViewFactory lvFactory = leafViewFactoryMap.get(viewType);
        if (lvFactory != null)
            return (I) lvFactory.create(rc, asset);
        if (parent == null)
            return null;
        return parent.createLeafView(viewType, rc, asset);
    }

    @Override
    public boolean isSubAsset() {
        return false;
    }

    @Override
    public boolean hasChildren() {
        return !children.isEmpty();
    }

    @Nullable
    @Override
    public <V> V getView(@NotNull Class<V> vClass) {
        View view = viewMap.get(vClass);
        return (V) view;
    }

    @NotNull
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @NotNull
    @Override
    public <V> V acquireView(@NotNull Class<V> viewType, RequestContext rc) throws
            AssetNotFoundException {
        synchronized (viewMap) {
            V view = getView(viewType);
            if (view != null) {
                return (V) view;
            }
            V leafView = createLeafView(viewType, rc, this);
            if (leafView != null)
                return addView(viewType, leafView);
            V wrappingView = createWrappingView(viewType, rc, this, null);
            if (wrappingView == null)
                throw new AssetNotFoundException("Unable to classify " + viewType.getName() + " context: " + rc);
            return addView(viewType, wrappingView);
        }
    }

    @Override
    public <V> V addView(Class<V> viewType, V v) {
        View view = (View) v;
        if (view.keyedView()) {
            keyedAsset = true;
        }
        viewMap.put(viewType, view);
        return v;
    }

    @Override
    public <I> void registerView(Class<I> viewType, I view) {
        viewMap.put(viewType, (View) view);
    }

    @NotNull
    @Override
    public Subscription subscription(boolean createIfAbsent) throws AssetNotFoundException {
        return createIfAbsent ? acquireView(ObjectSubscription.class, requestContext()) : getView(ObjectSubscription.class);
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

    @NotNull
    @Override
    public Asset acquireAsset(RequestContext context, @NotNull String fullName) throws AssetNotFoundException {
        if (keyedAsset != Boolean.TRUE) {
            int pos = fullName.indexOf("/");
            if (pos >= 0) {
                String name1 = fullName.substring(0, pos);
                String name2 = fullName.substring(pos + 1);
                return getAssetOrANFE(context, name1).acquireAsset(context, name2);
            }
        }
        return getAssetOrANFE(context, fullName);
    }

    @Nullable
    private Asset getAssetOrANFE(RequestContext context, @NotNull String name) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null) {
            asset = createAsset(context, name);
            if (asset == null)
                throw new AssetNotFoundException(name);
        }
        return asset;
    }

    @Nullable
    protected Asset createAsset(RequestContext context, @NotNull String name) {
        return children.computeIfAbsent(name, keyedAsset != Boolean.TRUE
                ? n -> new VanillaAsset(this, name)
                : n -> new VanillaSubAsset(context, this, name));
    }

    @Override
    public Asset getChild(String name) {
        return children.get(name);
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public String toString() {
        return fullName();
    }

    static class WrappingViewRecord<W, U> {
        final BiPredicate<RequestContext, Asset> predicate;
        final WrappingViewFactory<W, U> factory;
        final Class<U> underlyingType;

        WrappingViewRecord(BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
            this.predicate = predicate;
            this.factory = factory;
            this.underlyingType = underlyingType;
        }
    }
}
