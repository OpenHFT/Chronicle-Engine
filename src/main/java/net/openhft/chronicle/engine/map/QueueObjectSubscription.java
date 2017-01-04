/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.ISubscriber;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.SubscriptionStat;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by peter on 22/05/15.
 */
// todo review thread safety
public class QueueObjectSubscription<T, M> implements ObjectSubscription<T, M> {

    private static final Logger LOG = LoggerFactory.getLogger(QueueObjectSubscription.class);
    private final Set<TopicSubscriber<T, M>> topicSubscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<ExcerptTailer>> subscribers = new CopyOnWriteArraySet<>();
    private final Set<EventConsumer<T, M>> downstream = new CopyOnWriteArraySet<>();
    @Nullable
    private final SessionProvider sessionProvider;

    @Nullable
    private final Asset asset;
    private final Map<Subscriber, Subscriber> subscriptionDelegate = new IdentityHashMap<>();
    private final Class<T> topicType;

    @Nullable
    private Map<String, SubscriptionStat> subscriptionMonitoringMap = null;
    private EventLoop eventLoop;

    public QueueObjectSubscription(@NotNull RequestContext requestContext, @NotNull Asset asset) {
        this(requestContext.topicType(), requestContext.viewType(), asset);
    }

    public QueueObjectSubscription(Class topicType, @Nullable Class viewType, @Nullable Asset asset) {
        this.asset = asset;
        if (viewType != null && asset != null)
            asset.addView(viewType, this);

        sessionProvider = asset == null ? null : asset.findView(SessionProvider.class);

        eventLoop = asset.root().acquireView(EventLoop.class);
        this.topicType = topicType;
    }

    @Override
    public void close() {
        notifyEndOfSubscription(topicSubscribers);
        notifyEndOfSubscription(subscribers);
        //notifyEndOfSubscription(keySubscribers);
        notifyEndOfSubscription(downstream);
    }

    @Override
    public void onEndOfSubscription() {
        throw new UnsupportedOperationException("todo");
    }

    private void notifyEndOfSubscription(@NotNull Set<? extends ISubscriber> subscribers) {
        subscribers.forEach(this::notifyEndOfSubscription);
        subscribers.clear();
    }

    private void notifyEndOfSubscription(@NotNull ISubscriber subscriber) {
        try {
            subscriber.onEndOfSubscription();

        } catch (RuntimeException e) {
            Jvm.warn().on(getClass(), "Failed to send end of subscription", e);
        }
    }

    @Override
    public void setKvStore(KeyValueStore<T, M> kvStore) {
        //   throw new UnsupportedOperationException();
    }

    @Override
    public void notifyEvent(MapEvent<T, M> changeEvent) {
        // throw new UnsupportedOperationException();
    }

    @Override
    public int entrySubscriberCount() {
        return 0;
    }

    @Override
    public int topicSubscriberCount() {
        return topicSubscribers.size();
    }

    @Override
    public boolean hasSubscribers() {
        return !topicSubscribers.isEmpty() || !subscribers.isEmpty()
                || !downstream.isEmpty()
                || asset.hasChildren();
    }

    @Override
    public boolean needsPrevious() {
        // todo optimise this to reduce false positives.
        return !subscribers.isEmpty() || !downstream.isEmpty();
    }

    @Override
    public void registerSubscriber(@NotNull final RequestContext rc,
                                   @NotNull final Subscriber subscriber,
                                   @NotNull final Filter filter) {
        @NotNull final QueueView<T, M> chronicleQueue = asset.acquireView(QueueView.class, rc);

        @Nullable final T topic = ObjectUtils.convertTo(topicType, rc.name());
        eventLoop.addHandler(() -> {

            QueueView.Excerpt<T, M> excerpt = chronicleQueue.getExcerpt(topic);
            if (excerpt == null)
                return false;
            final M e = excerpt.message();
            if (e == null)
                return false;
            subscriber.accept(e);
            return true;
        });
    }

    @Override
    public void registerTopicSubscriber(@NotNull RequestContext rc, @NotNull final TopicSubscriber<T, M> subscriber) {
        addToStats("topicSubscription");

        topicSubscribers.add(subscriber);
        @NotNull AtomicBoolean terminate = new AtomicBoolean();

        @NotNull final ChronicleQueueView<T, M> chronicleQueue = (ChronicleQueueView) asset.acquireView
                (QueueView.class, rc);

        QueueView.Tailer<T, M> iterator = chronicleQueue.tailer();
        eventLoop.addHandler(() -> {

            // this will be set to true if onMessage throws InvalidSubscriberException
            if (terminate.get())
                throw new InvalidEventHandlerException();

            boolean busy = false;
            long start = System.nanoTime();
            do {
                @Nullable final QueueView.Excerpt<T, M> next = iterator.read();
                if (next == null)
                    return busy;
                try {
                    M message = next.message();
                    T topic = next.topic();
                    subscriber.onMessage(topic, message);

                } catch (InvalidSubscriberException e) {
                    topicSubscribers.add(subscriber);
                    terminate.set(true);

                } catch (RuntimeException e) {
                    Jvm.warn().on(getClass(), e);
                    terminate.set(true);
                }
                busy = true;
            } while (System.nanoTime() - start < 5000);
            return busy;
        });

    }

    @NotNull
    private T toT(@NotNull CharSequence eventName) {
        if (topicType == CharSequence.class)
            return (T) eventName;
        else if (topicType == String.class)
            return (T) eventName.toString();
        else if (topicType == WireKey.class)
            return (T) (WireKey) (() -> eventName.toString());
        else
            throw new UnsupportedOperationException("unable to convert " + eventName + " to type " + topicType);
    }

    @Override
    public void registerDownstream(@NotNull EventConsumer<T, M> subscription) {
        downstream.add(subscription);
    }

    public void unregisterDownstream(EventConsumer<T, M> subscription) {
        downstream.remove(subscription);
    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        final Subscriber delegate = subscriptionDelegate.get(subscriber);
        @NotNull final Subscriber s = delegate != null ? delegate : subscriber;
        boolean subscription = subscribers.remove(s);

        if (subscription) removeFromStats("subscription");

        s.onEndOfSubscription();
    }

    @Override
    public int keySubscriberCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerKeySubscriber(@NotNull RequestContext rc, @NotNull Subscriber<T> subscriber, @NotNull Filter<T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber subscriber) {
        topicSubscribers.remove(subscriber);
        removeFromStats("topicSubscription");
        subscriber.onEndOfSubscription();
    }

    //Needs some refactoring - need a definitive way of knowing when this map should become available
    //3 combinations, not lookedUP, exists or does not exist
    @Nullable
    private Map getSubscriptionMap() {
        if (subscriptionMonitoringMap != null) return subscriptionMonitoringMap;
        @Nullable Asset subscriptionAsset = asset.root().getAsset("proc/subscriptions");
        if (subscriptionAsset != null && subscriptionAsset.getView(MapView.class) != null) {
            subscriptionMonitoringMap = subscriptionAsset.getView(MapView.class);
        }
        return subscriptionMonitoringMap;
    }

    private void addToStats(String subType) {
        if (sessionProvider == null) return;

        @Nullable SessionDetails sessionDetails = sessionProvider.get();
        if (sessionDetails != null) {
            @Nullable String userId = sessionDetails.userId();

            @Nullable Map<String, SubscriptionStat> subStats = getSubscriptionMap();
            if (subStats != null) {
                SubscriptionStat stat = subStats.get(userId + "~" + subType);
                if (stat == null) {
                    stat = new SubscriptionStat();
                    stat.setFirstSubscribed(LocalTime.now());
                }
                stat.setTotalSubscriptions(stat.getTotalSubscriptions() + 1);
                stat.setActiveSubscriptions(stat.getActiveSubscriptions() + 1);
                stat.setRecentlySubscribed(LocalTime.now());
                subStats.put(userId + "~" + subType, stat);
            }
        }
    }

    private void removeFromStats(String subType) {
        if (sessionProvider == null) return;

        @Nullable SessionDetails sessionDetails = sessionProvider.get();
        if (sessionDetails != null) {
            @Nullable String userId = sessionDetails.userId();

            @Nullable Map<String, SubscriptionStat> subStats = getSubscriptionMap();
            if (subStats != null) {
                SubscriptionStat stat = subStats.get(userId + "~" + subType);
                if (stat == null) {
                    throw new AssertionError("There should be an active subscription");
                }
                stat.setActiveSubscriptions(stat.getActiveSubscriptions() - 1);
                stat.setRecentlySubscribed(LocalTime.now());
                subStats.put(userId + "~" + subType, stat);
            }
        }
    }
}
