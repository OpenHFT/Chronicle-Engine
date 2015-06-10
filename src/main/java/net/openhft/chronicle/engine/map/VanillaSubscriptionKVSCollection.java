package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static net.openhft.chronicle.engine.api.SubscriptionConsumer.notifyEachSubscriber;

/**
 * Created by peter on 22/05/15.
 */
// todo review thread safety
public class VanillaSubscriptionKVSCollection<K, MV, V> implements SubscriptionKVSCollection<K, MV, V> {
    final Set<TopicSubscriber<K, V>> topicSubscribers = new CopyOnWriteArraySet<>();
    final Set<Subscriber<KeyValueStore.Entry<K, V>>> subscribers = new CopyOnWriteArraySet<>();
    final Set<Subscriber<K>> keySubscribers = new CopyOnWriteArraySet<>();
    final Set<SubscriptionKVSCollection<K, MV, V>> downstream = new CopyOnWriteArraySet<>();


    KeyValueStore<K, MV, V> kvStore;

    boolean hasSubscribers = false;

    @Override
    public boolean keyedView() {
        return kvStore!=null;
    }

    public VanillaSubscriptionKVSCollection(KeyValueStore<K, MV, V> kvStore) {
        this.kvStore = kvStore;
    }

    public VanillaSubscriptionKVSCollection(RequestContext requestContext, @NotNull Asset asset, ThrowingSupplier<Assetted, AssetNotFoundException> assettedSupplier) {
        ((VanillaAsset) asset).addView(Subscription.class, this);
        ((VanillaAsset) asset).addView(SubscriptionKVSCollection.class, this);
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> kvStore) {
        this.kvStore = kvStore;
    }



    @Override
    public void notifyEvent(@NotNull MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException {
        if (hasSubscribers)
            notifyEvent0(mpe);
    }

    private void notifyEvent0(@NotNull MapReplicationEvent<K, V> mpe) {
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
    public void registerSubscriber(@NotNull RequestContext rc, Subscriber subscriber) {
        Boolean bootstrap = rc.bootstrap();
        Class eClass = rc.type();
        if (eClass == KeyValueStore.Entry.class || eClass == MapEvent.class) {
            subscribers.add((Subscriber) subscriber);
            if (bootstrap != Boolean.FALSE && kvStore != null) {
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
            if (bootstrap != Boolean.FALSE && kvStore != null) {
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
    public void registerTopicSubscriber(@NotNull RequestContext rc, @NotNull TopicSubscriber subscriber) {
        Boolean bootstrap = rc.bootstrap();
        topicSubscribers.add((TopicSubscriber<K, V>) subscriber);
        if (bootstrap != Boolean.FALSE && kvStore != null) {
            try {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.entriesFor(i, e -> subscriber.onMessage(e.key(), e.value()));
            } catch (InvalidSubscriberException dontAdd) {
                topicSubscribers.remove(subscriber);
            }
        }
        hasSubscribers = true;
    }

    @Override
    public void registerDownstream(Subscription subscription) {
        downstream.add((SubscriptionKVSCollection<K, MV, V>) subscription);
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
