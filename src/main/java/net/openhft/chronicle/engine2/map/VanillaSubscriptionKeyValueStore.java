package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.TopicSubscriber;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V> {
    final SubscriptionKVSCollection<K, MV, V> subscriptions = new SubscriptionKVSCollection<>(this);

    public VanillaSubscriptionKeyValueStore(FactoryContext<KeyValueStore<K, MV, V>> context) {
        super(context);
        MapView view = context.parent.getView(MapView.class);
        view.underlying(this);
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
    public <E> void registerTopicSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscriptions.registerTopicSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscriptions.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterTopicSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscriptions.unregisterTopicSubscriber(eClass, subscriber, query);
    }
}
