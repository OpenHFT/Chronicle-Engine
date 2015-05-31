package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

public class LocalAsset implements Asset, Assetted, Closeable {
    private final LocalSession session;
    private final String name;
    private final Asset parent;
    private final Asset underlying;
    private final Map<String, Asset> children = Collections.synchronizedMap(new HashMap<>());
    private final Map<Class, View> viewMap = new ConcurrentSkipListMap<>(VanillaAsset.CLASS_COMPARATOR);

    public LocalAsset(LocalSession session, String name, Asset parent, Asset underlying) {
        this.session = session;
        this.name = name;
        this.parent = parent;
        this.underlying = underlying;
        ((VanillaAsset) underlying).viewMap.forEach((type, view) -> viewMap.put(type, View.forSession(view, session, this)));
    }

    @Override
    public String name() {
        return name;
    }

    @Nullable
    @Override
    public Asset parent() {
        return parent;
    }

    @NotNull
    @Override
    public Stream<Asset> children() {
        throw new UnsupportedOperationException();
    }

    @Override
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
            ViewFactory<Asset> factory = acquireFactory(Asset.class);
            asset = factory.create(requestContext(name), this, () -> resource);
        }
        children.put(name, asset);
        return asset;
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

    private <A> Asset createAsset(String name, Class<A> assetClass, Class class1, Class class2) {
        return null;
    }

    @NotNull
    @Override
    public <A> Asset acquireChild(Class<A> assetClass, RequestContext context, String name) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> void addClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ViewLayer classify(Class viewType, RequestContext rc) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset getChild(String name) {
        return children.get(name);
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() {
        throw new UnsupportedOperationException();
    }

    <V> V acquireView0(Class viewClass, Function<Class<V>, View> builder) {
        synchronized (viewMap) {
            View view = viewMap.get(viewClass);
            if (view == null)
                viewMap.put(viewClass, view = builder.apply(viewClass));
            return (V) view;
        }
    }

    @Override
    public <V> V acquireView(Class<V> viewType, RequestContext rc) {
        View view = acquireView0(viewType, vc -> {
            V i = underlying.acquireView(viewType, rc);
            View i2 = (View) View.forSession(i, session, this);
            if (i2 instanceof SubscriptionKeyValueStore) {
                MapView mv = getView(MapView.class);
                if (mv != null)
                    mv.underlying(i2);
            }
            return i2;
        });
        return (V) view;
    }

    @Override
    public <V> V getView(Class<V> vClass) {
        return (V) viewMap.get(vClass);
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> ViewFactory<I> getFactory(Class<I> iClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> ViewFactory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, ViewFactory<I> factory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Subscription subscription(boolean createIfAbsent) {
        if (createIfAbsent) {
            RequestContext rc0 = requestContext();
            Subscription subscription0 = underlying.acquireView(Subscription.class, rc0);
            Subscription subscription = acquireView(Subscription.class, rc0);
            subscription0.registerDownstream(rc0, subscription);
            return subscription;
        } else {
            return getView(Subscription.class);
        }
    }

    @Override
    public Asset asset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void underlying(Object underlying) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object underlying() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }
}
