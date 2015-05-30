package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 22/05/15.
 */
// todo review thread safety
public class VanillaSubscriptionKVSCollection<K, MV, V> implements SubscriptionKVSCollection<K, V> {
    final Set<TopicSubscriber<K, V>> topicSubscribers = new CopyOnWriteArraySet<>();
    final Set<Subscriber<KeyValueStore.Entry<K, V>>> subscribers = new CopyOnWriteArraySet<>();
    final Set<Subscriber<K>> keySubscribers = new CopyOnWriteArraySet<>();
    final Set<SubscriptionKVSCollection<K, V>> downstream = new CopyOnWriteArraySet<>();
    final KeyValueStore<K, MV, V> kvStore;

    boolean hasSubscribers = false;

    public VanillaSubscriptionKVSCollection(KeyValueStore<K, MV, V> kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void notifyUpdate(K key, V oldValue, V value) {
        if (hasSubscribers)
            notifyUpdate0(key, oldValue, value);
    }

    private void notifyUpdate0(K key, V oldValue, V value) {
        if (!topicSubscribers.isEmpty()) {
            topicSubscribers.forEach(ts -> ts.onMessage(key, value));
        }
        if (!subscribers.isEmpty()) {
            if (oldValue == null) {
                InsertedEvent<K, V> inserted = InsertedEvent.of(key, value);
                subscribers.forEach(s -> s.onMessage(inserted));

            } else {
                UpdatedEvent<K, V> updated = UpdatedEvent.of(key, oldValue, value);
                subscribers.forEach(s -> s.onMessage(updated));
            }
        }
        if (!keySubscribers.isEmpty()) {
            keySubscribers.forEach(s -> s.onMessage(key));
        }
        if (!downstream.isEmpty()) {
            downstream.forEach(d -> d.notifyUpdate(key, oldValue, value));
        }
    }

    @Override
    public void notifyRemoval(K key, V oldValue) {
        if (hasSubscribers)
            notifyRemoval0(key, oldValue);
    }

    private void notifyRemoval0(K key, V oldValue) {
        if (!topicSubscribers.isEmpty()) {
            topicSubscribers.forEach(ts -> ts.onMessage(key, null));
        }
        if (!subscribers.isEmpty()) {
            RemovedEvent<K, V> removed = RemovedEvent.of(key, oldValue);
            subscribers.forEach(s -> s.onMessage(removed));
        }
        if (!keySubscribers.isEmpty()) {
            keySubscribers.forEach(s -> s.onMessage(key));
        }
        if (!downstream.isEmpty()) {
            downstream.forEach(d -> d.notifyRemoval(key, oldValue));
        }
    }

    @Override
    public boolean hasSubscribers() {
        return hasSubscribers;
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        boolean bootstrap = rc.bootstrap();
        Class eClass = rc.type();
        if (eClass == KeyValueStore.Entry.class || eClass == MapEvent.class) {
            subscribers.add((Subscriber) subscriber);
            if (bootstrap) {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.entriesFor(i, e -> subscriber.onMessage((E) InsertedEvent.of(e.key(), e.value())));
            }
        } else {
            keySubscribers.add((Subscriber<K>) subscriber);
            if (bootstrap) {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.keysFor(i, k -> subscriber.onMessage((E) k));
            }
        }
        hasSubscribers = true;
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        boolean bootstrap = rc.bootstrap();
        topicSubscribers.add((TopicSubscriber<K, V>) subscriber);
        if (bootstrap) {
            for (int i = 0; i < kvStore.segments(); i++)
                kvStore.entriesFor(i, (KeyValueStore.Entry<K, V> e) -> subscriber.onMessage((T) e.key(), (E) e.value()));
        }
        hasSubscribers = true;
    }

    @Override
    public void registerDownstream(RequestContext rc, Subscription subscription) {
        downstream.add((SubscriptionKVSCollection<K, V>) subscription);
        hasSubscribers = true;
    }

    @Override
    public void unregisterDownstream(RequestContext rc, Subscription subscription) {
        downstream.remove(subscription);
        updateHasSubscribers();
    }

    @Override
    public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
        subscribers.remove(subscriber);
        updateHasSubscribers();
    }

    @Override
    public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        topicSubscribers.remove(subscriber);
        updateHasSubscribers();
    }

    private void updateHasSubscribers() {
        hasSubscribers = !topicSubscribers.isEmpty() && !subscribers.isEmpty() && !downstream.isEmpty();
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }
}
