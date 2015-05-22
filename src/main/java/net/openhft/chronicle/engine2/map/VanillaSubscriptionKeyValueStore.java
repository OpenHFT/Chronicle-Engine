package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.TopicSubscriber;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, V> extends AbstractKeyValueStore<K, V> implements SubscriptionKeyValueStore<K, V> {
    final Set<TopicSubscriber<V>> topicSubscribers = new CopyOnWriteArraySet<>();
    final Set<Subscriber<Entry<K, V>>> subscribers = new CopyOnWriteArraySet<>();
    boolean hasSubscribers = false;

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);
        if (hasSubscribers) {
            if (!topicSubscribers.isEmpty()) {
                String key2 = key.toString();
                topicSubscribers.forEach(ts -> ts.on(key2, value));
            }
            if (!subscribers.isEmpty()) {
                if (oldValue == null) {
                    InsertEvent<K, V> inserted = new InsertEvent<>(key, value);
                    subscribers.forEach(s -> s.on(inserted));
                } else {
                    UpdatedEvent<K, V> updated = new UpdatedEvent<>(key, oldValue, value);
                    subscribers.forEach(s -> s.on(updated));
                }
            }
        }
        return oldValue;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (hasSubscribers && oldValue != null) {
            if (!topicSubscribers.isEmpty()) {
                String key2 = key.toString();
                topicSubscribers.forEach(ts -> ts.on(key2, oldValue));
            }
            if (!subscribers.isEmpty()) {
                RemovedEvent<K, V> removed = new RemovedEvent<>(key, oldValue);
                subscribers.forEach(s -> s.on(removed));
            }
        }
        return oldValue;
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber) {
        topicSubscribers.add((TopicSubscriber<V>) subscriber);
        hasSubscribers = true;
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber) {
        topicSubscribers.remove(subscriber);
        hasSubscribers = !topicSubscribers.isEmpty() && !subscribers.isEmpty();
    }
}
