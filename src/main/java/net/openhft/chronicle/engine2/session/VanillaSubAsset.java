package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.SubAsset;
import net.openhft.chronicle.engine2.pubsub.SimpleSubscription;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<E> implements SubAsset<E>, Closeable, TopicSubscriber<String, E> {
    private final Asset parent;
    private final String name;
    private final SimpleSubscription<E> subscription = new SimpleSubscription<>();

    VanillaSubAsset(RequestContext context, Asset asset) {
        this(asset, context.name());
    }

    VanillaSubAsset(Asset parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    @Override
    public Subscription subscription(boolean createIfAbsent) {
        return subscription;
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> void addClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V getView(Class<V> vClass) {
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
        if (viewType == Publisher.class || viewType == Reference.class) {
            return (V) parent.acquireFactory(viewType).create(requestContext().type(rc.type()).fullName(name), this, () ->
                            acquireView(requestContext().viewType(MapView.class).type(String.class).type2(rc.type()))
            );
        }
        throw new UnsupportedOperationException("todo vClass: " + viewType + ", rc: " + rc);
    }

    @Override
    public ViewLayer classify(Class viewType, RequestContext rc) {
        throw new UnsupportedOperationException("todo");
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
    public Stream<Asset> children() {
        return Stream.of();
    }

    @NotNull
    @Override
    public <A> Asset acquireChild(Class<A> assetClass, RequestContext context, String name) throws AssetNotFoundException {
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
}
