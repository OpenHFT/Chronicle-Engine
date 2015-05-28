package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V> {
    final SubscriptionKVSCollection<K, MV, V> subscriptions = new SubscriptionKVSCollection<>(this);

    public VanillaSubscriptionKeyValueStore(RequestContext<KeyValueStore<K, MV, V>> context) {
        this(context.item());
    }

    VanillaSubscriptionKeyValueStore(KeyValueStore<K, MV, V> item) {
        super(item);
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return new VanillaSubscriptionKeyValueStore<>(View.forSession(kvStore, session, asset));
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);
        subscriptions.notifyUpdate(key, oldValue, value);
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null)
            subscriptions.notifyRemoval(key, oldValue);
        return oldValue;
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        subscriptions.registerSubscriber(rc, subscriber);
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        subscriptions.registerTopicSubscriber(rc, subscriber);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscriptions.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        subscriptions.unregisterTopicSubscriber(tClass, eClass, subscriber, query);
    }
}
