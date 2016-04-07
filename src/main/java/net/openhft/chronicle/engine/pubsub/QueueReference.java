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

    private final Map<Subscriber<M>, AtomicBoolean> subscribers = new HashMap<>();

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

            final QueueView.Excerpt<T, M> next = iterator.read();

            if (next == null)
                return false;
            try {
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
    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public Class getType() {
        return eClass;
    }


}

