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

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueReference<T, M> implements Reference<M> {

    private final Class<M> eClass;
    private final ChronicleQueueView<T, M> chronicleQueue;
    private final T name;
    private final Asset asset;
    private final Map<Subscriber<M>, AtomicBoolean> subscribers = new HashMap<>();
    private EventLoop eventLoop;
    private QueueView.Tailer<T, M> tailer;

    public QueueReference(Class type, Asset asset, QueueView<T, M> chronicleQueue, T name) {
        this.eClass = type;
        this.chronicleQueue = (ChronicleQueueView) chronicleQueue;
        this.name = name;
        eventLoop = asset.root().acquireView(EventLoop.class);
        this.asset = asset;
        tailer = this.chronicleQueue.tailer();
    }

    public QueueReference(RequestContext requestContext, Asset asset, QueueView<T, M> queueView) {
        this(requestContext.type(), asset, queueView,
                (T) ObjectUtils.convertTo(requestContext.type(), requestContext.name()));
    }

    @Override
    public long set(M event) {
        return chronicleQueue.publishAndIndex(name, event);
    }

    @Nullable
    @Override
    public M get() {
        final QueueView.Excerpt<T, M> next = tailer.read();
        if (next == null)
            return null;
        return next.message();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSubscriber(boolean bootstrap,
                                   int throttlePeriodMs,
                                   Subscriber<M> subscriber) throws AssetNotFoundException {

        AtomicBoolean terminate = new AtomicBoolean();
        subscribers.put(subscriber, terminate);

        final ChronicleQueueView<T, M> chronicleQueue = (ChronicleQueueView<T, M>) asset.acquireView(QueueView.class);

        final QueueView.Tailer<T, M> iterator = chronicleQueue.tailer();

        eventLoop.addHandler(() -> {

            // this will be set to true if onMessage throws InvalidSubscriberException
            if (terminate.get())
                throw new InvalidEventHandlerException();

            final QueueView.Excerpt<T, M> item = iterator.read();

            if (item == null || item.index() == -1)
                return false;
            try {
                subscriber.onMessage(item.message());
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
    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public Class getType() {
        return eClass;
    }
}

