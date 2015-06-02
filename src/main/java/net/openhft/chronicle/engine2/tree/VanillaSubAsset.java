package net.openhft.chronicle.engine2.tree;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.SubAsset;
import net.openhft.chronicle.engine2.pubsub.SimpleSubscription;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<E> implements SubAsset<E>, Closeable, TopicSubscriber<String, E>, Supplier<E> {
    private final Asset parent;
    private final String name;
    private final SimpleSubscription<E> subscription;
    private Reference<E> reference;

    VanillaSubAsset(Asset parent, String name) {
        this.parent = parent;
        this.name = name;
        subscription = new SimpleSubscription<>(this);
    }

    @Override
    public Subscription subscription(boolean createIfAbsent) {
        return subscription;
    }

    @Override
    public <V> void addClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory) {
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
    public <V> V acquireView(Class<V> viewType, RequestContext rc) {
        if (viewType == Reference.class || viewType == Supplier.class) {
            if (reference == null)
                reference = (Reference<E>) acquireViewFor(viewType, rc);
            return (V) reference;
        }
        if (viewType == Publisher.class) {
            if (reference == null)
                return (V) acquireViewFor(viewType, rc);
            return (V) reference;
        }
        if (viewType == Subscription.class)
            return (V) subscription;
        throw new UnsupportedOperationException("todo vClass: " + viewType + ", rc: " + rc);
    }

    private <V> V acquireViewFor(Class<V> viewType, RequestContext rc) {
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
    public Asset acquireAsset(String name) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset getAsset(String name) {
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

    @Override
    public E get() {
        if (reference == null)
            reference = acquireView(Reference.class, requestContext());
        return reference.get();
    }
}
