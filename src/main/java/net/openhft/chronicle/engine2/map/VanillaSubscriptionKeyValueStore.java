package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.TopicSubscriber;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V> {
    final SubscriptionKVSCollection<K, MV, V> subscriptions = new SubscriptionKVSCollection<>(this);

    public VanillaSubscriptionKeyValueStore(FactoryContext<KeyValueStore<K, MV, V>> context) {
        super(context);
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
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscriptions.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        subscriptions.registerTopicSubscriber(tClass, eClass, subscriber, query);
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
