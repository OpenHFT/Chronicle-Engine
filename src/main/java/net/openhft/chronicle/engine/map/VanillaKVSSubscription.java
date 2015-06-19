/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer.notifyEachSubscriber;

/**
 * Created by peter on 22/05/15.
 */
// todo review thread safety
public class VanillaKVSSubscription<K, MV, V> implements ObjectKVSSubscription<K, MV, V>, RawKVSSubscription<K, MV, V> {
    private final Set<TopicSubscriber<K, V>> topicSubscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<KeyValueStore.Entry<K, V>>> subscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<K>> keySubscribers = new CopyOnWriteArraySet<>();
    private final Set<EventConsumer<K, V>> downstream = new CopyOnWriteArraySet<>();
    private final Asset asset;
    private KeyValueStore<K, MV, V> kvStore;
    private boolean hasSubscribers = false;

    public VanillaKVSSubscription(RequestContext requestContext, Asset asset) {
        this(requestContext.viewType(), asset);
    }

    public VanillaKVSSubscription(Class viewType, Asset asset) {
        this.asset = asset;
        if (viewType != null)
        asset.addView(viewType, this);
    }

    @Override
    public void close() {
        notifyEndOfSubscription(topicSubscribers);
        notifyEndOfSubscription(subscribers);
        notifyEndOfSubscription(keySubscribers);
        notifyEndOfSubscription(downstream);
    }

    @Override
    public void onEndOfSubscription() {
        throw new UnsupportedOperationException("todo");
    }

    private void notifyEndOfSubscription(Set<? extends ISubscriber> subscribers) {
        for (ISubscriber subscriber : subscribers) {
            notifyEndOfSubscription(subscriber);
        }
        subscribers.clear();
    }

    private void notifyEndOfSubscription(ISubscriber subscriber) {
        try {
            subscriber.onEndOfSubscription();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean keyedView() {
        return kvStore != null;
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void notifyEvent(@NotNull MapEvent<K, V> changeEvent) {
        if (hasSubscribers())
            notifyEvent0(changeEvent);
    }

    @Override
    public int keySubscriberCount() {
        return keySubscribers.size();
    }

    @Override
    public int entrySubscriberCount() {
        return subscribers.size();
    }

    @Override
    public int topicSubscriberCount() {
        return topicSubscribers.size();
    }

    private boolean hasSubscribers() {
        return hasSubscribers || asset.hasChildren();
    }

    private void notifyEvent0(@NotNull MapEvent<K, V> changeEvent) {
        notifyEvent1(changeEvent);
        notifyEventToChild(changeEvent);
    }

    private void notifyEvent1(@NotNull MapEvent<K, V> changeEvent) {
        K key = changeEvent.key();

        if (!topicSubscribers.isEmpty()) {
            V value = changeEvent.value();
            notifyEachSubscriber(topicSubscribers, ts -> ts.onMessage(key, value));
        }
        if (!subscribers.isEmpty()) {
            notifyEachSubscriber(subscribers, s -> s.onMessage(changeEvent));
        }
        if (!keySubscribers.isEmpty()) {
            notifyEachSubscriber(keySubscribers, s -> s.onMessage(key));
        }
        if (!downstream.isEmpty()) {
            notifyEachSubscriber(downstream, d -> d.notifyEvent(changeEvent));
        }
    }

    private void notifyEventToChild(@NotNull MapEvent<K, V> changeEvent) {
        K key = changeEvent.key();
        if (asset.hasChildren() && key instanceof CharSequence) {
            String keyStr = key.toString();
            Asset child = asset.getChild(keyStr);
            if (child != null) {
                Subscription subscription = child.subscription(false);
                if (subscription instanceof SimpleSubscription) {
//                    System.out.println(changeEvent.toString().substring(0, 100));
                    ((SimpleSubscription) subscription).notifyMessage(changeEvent.value());
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
        if (eClass == KeyValueStore.Entry.class || eClass == MapEvent.class || eClass == MapEvent.class) {
            subscribers.add((Subscriber) subscriber);
            if (bootstrap != Boolean.FALSE && kvStore != null) {
                Subscriber<MapEvent<K, V>> sub = (Subscriber<MapEvent<K, V>>) subscriber;
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
    public void registerDownstream(EventConsumer<K, V> subscription) {
        downstream.add(subscription);
        hasSubscribers = true;
    }

    public void unregisterDownstream(EventConsumer<K, V> subscription) {
        downstream.remove(subscription);
        updateHasSubscribers();
    }

    @Override
    public void unregisterSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
        updateHasSubscribers();

    }

    @Override
    public void unregisterTopicSubscriber(TopicSubscriber subscriber) {
        topicSubscribers.remove(subscriber);
        updateHasSubscribers();
    }

    private void updateHasSubscribers() {
        hasSubscribers = !topicSubscribers.isEmpty() && !subscribers.isEmpty() && !downstream.isEmpty();
    }
}
