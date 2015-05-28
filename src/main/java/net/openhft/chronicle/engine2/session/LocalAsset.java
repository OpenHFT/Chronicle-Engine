package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
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
            Factory<Asset> factory = acquireFactory(Asset.class);
            asset = factory.create(requestContext(this).fullName(name).item(resource));
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
    public <A> Asset acquireChild(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
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

    @Override
    public <I> I acquireView(Class<I> vClass, Class class1, Class class2, String queryString) {
        viewMap.computeIfAbsent(vClass, vc -> {
            I i = underlying.acquireView(vClass, class1, class2, queryString);
            View i2 = (View) View.forSession(i, session, this);
            return i2;
        });
        throw new UnsupportedOperationException("todo vClass: " + vClass + ", class1: " + class1 + ", class2: " + class2 + ", query: " + queryString);
    }

    @Override
    public <V> V getView(Class<V> vClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> Factory<I> getFactory(Class<I> iClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> Factory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, Factory<I> factory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object item() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asset(Asset asset) {
        throw new UnsupportedOperationException();
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
