package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static net.openhft.chronicle.engine.api.SubscriptionConsumer.notifyEachSubscriber;

/**
 * Created by peter on 22/05/15.
 */
// todo review thread safety
public class VanillaSubscriptionKVSCollection<K, MV, V> implements SubscriptionKVSCollection<K, MV, V> {
    private final Set<TopicSubscriber<K, V>> topicSubscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<KeyValueStore.Entry<K, V>>> subscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<K>> keySubscribers = new CopyOnWriteArraySet<>();
    private final Set<SubscriptionKVSCollection<K, MV, V>> downstream = new CopyOnWriteArraySet<>();
    private final Asset asset;
    private KeyValueStore<K, MV, V> kvStore;
    private boolean hasSubscribers = false;

    @Override
    public boolean keyedView() {
        return kvStore!=null;
    }

    public VanillaSubscriptionKVSCollection(RequestContext requestContext, Asset asset) {
        this(asset);
    }

    public VanillaSubscriptionKVSCollection(Asset asset) {
        this.asset = asset;
        asset.addView(Subscription.class, this);
        asset.addView(SubscriptionKVSCollection.class, this);
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void notifyEvent(@NotNull MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException {
        if (hasSubscribers())
            notifyEvent0(mpe);
    }

    private boolean hasSubscribers() {
        return hasSubscribers || asset.hasChildren();
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
        if (asset.hasChildren() && key instanceof CharSequence) {
            String keyStr = key.toString();
            Asset child = asset.getChild(keyStr);
            if (child != null) {
                Subscription subscription = child.subscription(false);
                if (subscription instanceof SimpleSubscription) {
//                    System.out.println(mpe.toString().substring(0, 100));
                    ((SimpleSubscription) subscription).notifyMessage(mpe.value());
                }
            }
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
