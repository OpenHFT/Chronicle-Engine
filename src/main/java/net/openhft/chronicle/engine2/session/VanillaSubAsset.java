package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.SubAsset;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<T> implements SubAsset<T>, Closeable, TopicSubscriber<T> {
    private final Asset parent;
    private final String name;
    private final Set<Subscriber<T>> subscribers = new CopyOnWriteArraySet<>();

    VanillaSubAsset(Asset parent, String name, String query) {
        this.parent = parent;
        this.name = name;
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
    public <V> V acquireView(Class<V> vClass, String queryString) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, String queryString) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <V> V acquireView(Class<V> vClass, Class class1, Class class2, String queryString) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <I extends Interceptor> I acquireInterceptor(Class<I> iClass) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscribers.add((Subscriber) subscriber);
        parent().registerSubscriber(eClass, (TopicSubscriber<E>) this, query);
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscribers.remove((Subscriber) subscriber);
        if (subscribers.isEmpty())
            parent().unregisterSubscriber(eClass, (TopicSubscriber<E>) this, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
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

    public <I extends Interceptor> void registerInterceptor(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void on(String name, T t) {
        if (name.equals(this.name))
            subscribers.forEach(s -> s.on(t));
    }
}
