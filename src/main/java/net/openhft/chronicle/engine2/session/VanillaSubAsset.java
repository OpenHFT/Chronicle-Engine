package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.SubAsset;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine2.api.FactoryContext.factoryContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<T> implements SubAsset<T>, Closeable, TopicSubscriber<String, T> {
    private final Asset parent;
    private final String name;
    private final Set<Subscriber<T>> subscribers = new CopyOnWriteArraySet<>();

    VanillaSubAsset(FactoryContext context) {
        this.parent = context.parent();
        this.name = context.name();
    }

    @Override
    public Object item() {
        return parent.getView(KeyValueStore.class).get(name);
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
    public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
        if (vClass == Publisher.class) {
            MapView parentMap = parent.acquireView(MapView.class, String.class, class1, queryString);
            return (V) parent.acquireFactory(Publisher.class).create(factoryContext(this).type(class1).name(name).item(parentMap));
        }
        throw new UnsupportedOperationException("todo vClass: " + vClass + ", class1: " + class1 + ", class2: " + class2 + ", queryString: " + queryString);
    }

    @Override
    public <I> Factory<I> getFactory(Class<I> iClass) {
        return parent.getFactory(iClass);
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscribers.add((Subscriber) subscriber);
        parent().registerTopicSubscriber(String.class, eClass, (TopicSubscriber<String, E>) this, query);
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscribers.remove((Subscriber) subscriber);
        if (subscribers.isEmpty())
            parent().unregisterTopicSubscriber(String.class, eClass, (TopicSubscriber<String, E>) this, query);
    }

    @Override
    public <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
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
    public <A> Asset acquireChild(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset getChild(String name) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void removeChild(String name) {
        throw new UnsupportedOperationException("todo");
    }

    public Asset add(String name, Assetted resource) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void onMessage(String name, T t) {
        if (name.equals(this.name))
            subscribers.forEach(s -> s.on(t));
    }

    @Override
    public <I> Factory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <I> void registerFactory(Class<I> iClass, Factory<I> factory) {
        throw new UnsupportedOperationException("todo");
    }
}
