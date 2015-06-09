package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubAsset;
import net.openhft.chronicle.engine.api.map.ValueReader;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<E> implements SubAsset<E>, Closeable, TopicSubscriber<String, E> {
    private final Asset parent;
    private final String name;
    private final SimpleSubscription<E> subscription;
    private Reference<E> reference;

    VanillaSubAsset(RequestContext rc, Asset parent, String name) throws AssetNotFoundException {
        this.parent = parent;
        this.name = name;
        reference = (Reference<E>) acquireViewFor(Reference.class, rc);
        ValueReader valueReader;
        try {
            valueReader = parent.acquireView(ValueReader.class, rc);
        } catch (Exception e) {
            valueReader = ValueReader.PASS;
        }
        subscription = new SimpleSubscription<>(reference, valueReader == null ? ValueReader.PASS : valueReader);
    }

    @Override
    public Subscription subscription(boolean createIfAbsent) {
        return subscription;
    }

    @Override
    public <V> void addClassifier(Class<V> assetType, String name, ThrowingFunction<RequestContext, ViewLayer, AssetNotFoundException> viewBuilderFactory) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V getView(Class<V> vClass) {
        if (vClass == Reference.class || vClass == Publisher.class || vClass == Supplier.class)
            return (V) reference;
        if (vClass == Subscription.class || vClass == SimpleSubscription.class)
            return (V) subscription;
        throw new UnsupportedOperationException("todo");
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
    public <V> V acquireView(Class<V> viewType, RequestContext rc) throws AssetNotFoundException {
        if (viewType == Reference.class || viewType == Supplier.class) {
            return (V) reference;
        }
        if (viewType == Publisher.class) {
            if (reference == null)
                return (V) acquireViewFor(viewType, rc);
            return (V) reference;
        }
        if (viewType == Subscription.class) {
            return (V) subscription;
        }
        throw new UnsupportedOperationException("todo vClass: " + viewType + ", rc: " + rc);
    }

    private <V> V acquireViewFor(Class<V> viewType, RequestContext rc) throws AssetNotFoundException {
        return parent.acquireFactory(viewType).create(requestContext().type(rc.type()).fullName(name), this, () ->
                parent.getView(MapView.class));
    }

    @Override
    public ViewLayer classify(Class viewType, RequestContext rc) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean isSubAsset() {
        return true;
    }

    @Override
    public <I> ViewFactory<I> getFactory(Class<I> iClass) {
        return parent.getFactory(iClass);
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException("todo");
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
    public Asset acquireAsset(RequestContext context, String fullName) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset getAsset(String fullName) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset getChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException("todo");
    }

    public Asset add(String name, Assetted resource) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void onMessage(String name, E e) {
        if (name.equals(this.name))
            subscription.notifyMessage(e);
    }

    @Override
    public <I> ViewFactory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, ViewFactory<I> factory) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean keyedView() {
        return false;
    }
}
