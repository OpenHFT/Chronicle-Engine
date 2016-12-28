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

package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Created by peter on 29/05/15.
 */
public class QueueSimpleSubscription<E> implements SimpleSubscription<E> {

    private static final Logger LOG = LoggerFactory.getLogger(QueueSimpleSubscription.class);
    private final Map<Subscriber<E>, AtomicBoolean> subscribers = new ConcurrentHashMap<>();
    private final Function<Object, E> valueReader;

    // private final ObjectSubscription objectSubscription;

    @NotNull
    private final ChronicleQueueView<?, E> chronicleQueue;
    @NotNull
    private final EventLoop eventLoop;
    private final String topic;

    public QueueSimpleSubscription(Function<Object, E> valueReader,
                                   @NotNull Asset parent, String topic) {
        this.valueReader = valueReader;
        this.topic = topic;
        chronicleQueue = (ChronicleQueueView) parent.acquireView(QueueView.class);
        eventLoop = parent.acquireView(EventLoop.class);
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc,
                                   @NotNull Subscriber<E> subscriber,
                                   @NotNull Filter<E> filter) {
        registerSubscriber(false, 0, subscriber);
    }

    public void registerSubscriber(boolean bootstrap,
                                   int throttlePeriodMs,
                                   @NotNull Subscriber<E> subscriber) throws AssetNotFoundException {

        @NotNull AtomicBoolean terminate = new AtomicBoolean();
        subscribers.put(subscriber, terminate);

        @Nullable final QueueView.Tailer<?, E> tailer = chronicleQueue.tailer();

        eventLoop.addHandler(() -> {

            // this will be set to true if onMessage throws InvalidSubscriberException
            if (terminate.get())
                throw new InvalidEventHandlerException();

            @Nullable final QueueView.Excerpt<?, E> next = tailer.read();

            if (next == null)
                return false;
            try {
                Object topic = next.topic();

                if (!this.topic.equals(topic.toString()))
                    return true;

                subscriber.onMessage(next.message());
            } catch (InvalidSubscriberException e) {
                terminate.set(true);
            }

            return true;
        });

    }

    @Override
    public void unregisterSubscriber(Subscriber subscriber) {
        final AtomicBoolean terminator = subscribers.remove(subscriber);
        if (terminator != null)
            terminator.set(true);
    }

    @Override
    public int keySubscriberCount() {
        return subscriberCount();
    }

    @Override
    public int entrySubscriberCount() {
        return 0;
    }

    @Override
    public int topicSubscriberCount() {
        return 0;
    }

    @Override
    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public void notifyMessage(Object e) {
        try {
            @NotNull E ee = e instanceof BytesStore ? valueReader.apply(e) : (E) e;
            //SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(ee));
        } catch (ClassCastException e1) {
            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "Is " + valueReader + " the correct ValueReader?");
            throw e1;
        }
    }

    @Override
    public void close() {
        for (@NotNull Subscriber<E> subscriber : subscribers.keySet()) {
            try {
                subscriber.onEndOfSubscription();
            } catch (Exception e) {
                Jvm.debug().on(getClass(), e);
            }
        }
    }
}
