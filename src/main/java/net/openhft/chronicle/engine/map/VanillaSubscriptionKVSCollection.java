package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static net.openhft.chronicle.engine.api.SubscriptionConsumer.notifyEachSubscriber;

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
    public void notifyEvent(MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException {
        if (hasSubscribers)
            notifyEvent0(mpe);
    }

    private void notifyEvent0(MapReplicationEvent<K, V> mpe) {
        K key = mpe.key();

        if (!topicSubscribers.isEmpty()) {
            V value = mpe.value();
            notifyEachSubscriber(topicSubscribers, ts -> ts.onMessage(key, value));
        }
        if (!subscribers.isEmpty()) {
            notifyEachSubscriber(subscribers, s -> s.onMessage(mpe));
        }
        if (!keySubscribers.isEmpty()) {
            notifyEachSubscriber(keySubscribers, s -> s.onMessage(key));
        }
        if (!downstream.isEmpty()) {
            notifyEachSubscriber(downstream, d -> d.notifyEvent(mpe));
        }
    }

    @Override
    public boolean needsPrevious() {
        // todo optimise this to reduce false positives.
        return !subscribers.isEmpty() || !downstream.isEmpty();
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        Boolean bootstrap = rc.bootstrap();
        Class eClass = rc.type();
        if (eClass == KeyValueStore.Entry.class || eClass == MapEvent.class) {
            subscribers.add((Subscriber) subscriber);
            if (bootstrap != Boolean.FALSE) {
                Subscriber<MapReplicationEvent<K, V>> sub = (Subscriber<MapReplicationEvent<K, V>>) subscriber;
                try {
                    for (int i = 0; i < kvStore.segments(); i++)
                        kvStore.entriesFor(i, sub::onMessage);
                } catch (InvalidSubscriberException e) {
                    subscribers.remove(subscriber);
                }
            }
        } else {
            Subscriber<K> sub = (Subscriber<K>) subscriber;
            keySubscribers.add(sub);
            if (bootstrap != Boolean.FALSE) {
                try {
                    for (int i = 0; i < kvStore.segments(); i++)
                        kvStore.keysFor(i, sub::onMessage);
                } catch (InvalidSubscriberException e) {
                    subscribers.remove(subscriber);
                }
            }
        }
        hasSubscribers = true;
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        Boolean bootstrap = rc.bootstrap();
        topicSubscribers.add((TopicSubscriber<K, V>) subscriber);
        if (bootstrap != Boolean.FALSE) {
            try {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.entriesFor(i, e -> subscriber.onMessage((T) e.key(), (E) e.value()));
            } catch (InvalidSubscriberException dontAdd) {
                topicSubscribers.remove(subscriber);
            }
        }
        hasSubscribers = true;
    }

    @Override
    public void registerDownstream(Subscription subscription) {
        downstream.add((SubscriptionKVSCollection<K, V>) subscription);
        hasSubscribers = true;
    }

    public void unregisterDownstream(Subscription subscription) {
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
}
