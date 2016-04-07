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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;
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

    private final QueueView<?, E> chronicleQueue;
    private final EventLoop eventLoop;
    private final String topic;

    public QueueSimpleSubscription(Function<Object, E> valueReader,
                                   Asset parent, String topic) {
        this.valueReader = valueReader;
        this.topic = topic;
        chronicleQueue = parent.acquireView(QueueView.class);
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
                                   Subscriber<E> subscriber) throws AssetNotFoundException {

        AtomicBoolean terminate = new AtomicBoolean();
        subscribers.put(subscriber, terminate);


        final QueueView.Iterator<?, E> iterator = chronicleQueue.iterator();

        eventLoop.addHandler(() -> {

            // this will be set to true if onMessage throws InvalidSubscriberException
            if (terminate.get())
                throw new InvalidEventHandlerException();

            final QueueView.Excerpt<?, E> next = iterator.next();

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
            E ee = e instanceof BytesStore ? valueReader.apply(e) : (E) e;
            //SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(ee));
        } catch (ClassCastException e1) {
            System.err.println("Is " + valueReader + " the correct ValueReader?");
            throw e1;
        }
    }

    @Override
    public void close() {
        for (Subscriber<E> subscriber : subscribers.keySet()) {
            try {
                subscriber.onEndOfSubscription();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }
}
