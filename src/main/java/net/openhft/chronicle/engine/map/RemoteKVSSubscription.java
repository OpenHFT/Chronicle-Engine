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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.subscribe;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.unSubscribe;
import static net.openhft.chronicle.engine.server.internal.SubscriptionHandlerProcessor.EventId.*;
import static net.openhft.chronicle.wire.CoreFields.reply;

public class RemoteKVSSubscription<K, MV, V> implements ObjectKVSSubscription<K, MV, V>,
        Closeable {

    private final MapSubscriptionHandler mapSubscriptionHandler;
    private final SubscriptionHandler subscriptionHandler;
    private final TcpChannelHub hub;

    private class MapSubscriptionHandler extends AbstractStatelessClient {
        Map<Subscriber<MapEvent<K, V>>, Long> topicSubscribersToTid = new ConcurrentHashMap<>();

        public MapSubscriptionHandler(RequestContext context, TcpChannelHub view) {
            super(view, (long) 0, toMapUri(context));
        }

        private void registerSubscriber(RequestContext rc, Subscriber<MapEvent<K, V>> subscriber) {
            final long startTime = System.currentTimeMillis();

            if (hub.outBytesLock().isHeldByCurrentThread())
                throw new IllegalStateException("Cannot view map while debugging");

            hub.outBytesLock().lock();
            try {
                tid = writeMetaDataStartTime(startTime);
                topicSubscribersToTid.put(subscriber, tid);
                hub.outWire().writeDocument(false, wireOut ->
                        wireOut.writeEventName(subscribe).
                                typeLiteral(CLASS_ALIASES.nameFor(rc.elementType())));

                hub.writeSocket(hub.outWire());
            } finally {
                hub.outBytesLock().unlock();
            }

            assert !hub.outBytesLock().isHeldByCurrentThread();
            hub.asyncReadSocket(tid, w -> w.readDocument(null, d -> {
                ValueIn read = d.read(reply);

                final Class aClass = rc.elementType();

                final Object object = (MapEvent.class.isAssignableFrom(aClass)) ? read
                        .typedMarshallable()
                        : read.object(rc.elementType());

                this.onEvent(object, subscriber);

            }));
        }

        private void onEvent(Object message, Subscriber subscriber) {
            try {
                if (message == null) {
                    // todo remove subscriber.
                } else {
                    subscriber.onMessage(message);
                }
            } catch (InvalidSubscriberException noLongerValid) {
                unregisterSubscriber(subscriber);
            }
        }

        private void unregisterSubscriber(Subscriber<MapEvent<K, V>> subscriber) {
            Long tid = topicSubscribersToTid.get(subscriber);
            if (tid == -1) {
                LOG.warn("There is subscription to unsubscribe");
            }

            hub.outBytesLock().lock();
            try {
                writeMetaDataForKnownTID(tid);
                hub.outWire().writeDocument(false, wireOut -> {
                    wireOut.writeEventName(unSubscribe).text("");
                });

                hub.writeSocket(hub.outWire());
            } finally {
                hub.outBytesLock().unlock();
            }
        }

    }

    private class SubscriptionHandler extends AbstractStatelessClient {

        public SubscriptionHandler(RequestContext context, TcpChannelHub view) {
            super(view, (long) 0, toSubscriptionUri(context));
        }

        Map<TopicSubscriber, Long> topicSubscribersToTid = new ConcurrentHashMap<>();

        private void registerTopicSubscriber(RequestContext rc, TopicSubscriber<K, V> subscriber) {
            final long startTime = System.currentTimeMillis();

            if (hub.outBytesLock().isHeldByCurrentThread())
                throw new IllegalStateException("Cannot view map while debugging");

            hub.outBytesLock().lock();
            try {
                tid = writeMetaDataStartTime(startTime);
                topicSubscribersToTid.put(subscriber, tid);
                hub.outWire().writeDocument(false, wireOut ->
                        wireOut.writeEventName(registerTopicSubscriber).marshallable(m -> {
                            m.write(() -> "keyType").typeLiteral(rc.keyType());
                            m.write(() -> "valueType").typeLiteral(rc.valueType());

                        }));

                hub.writeSocket(hub.outWire());
            } finally {
                hub.outBytesLock().unlock();
            }

            assert !hub.outBytesLock().isHeldByCurrentThread();
            hub.asyncReadSocket(tid, w -> w.readDocument(null, d -> {
                ValueIn valueIn = d.read(reply);
                valueIn.marshallable(m -> {
                    final String topic = m.read(() -> "topic").text();
                    final ReadMarshallable message = m.read(() -> "message").typedMarshallable();
                    this.onEvent(topic, message, subscriber);
                });
            }));
        }

        private void onEvent(Object topic, Object message, TopicSubscriber subscriber) {
            try {
                if (message == null)
                    unregisterTopicSubscriber(subscriber);
                else
                    subscriber.onMessage(topic, message);
            } catch (InvalidSubscriberException noLongerValid) {
                unregisterTopicSubscriber(subscriber);
            }
        }

        private int topicSubscriberCount() {
            return proxyReturnInt(topicSubscriberCount);
        }

        private int keySubscriberCount() {
            return proxyReturnInt(keySubscriberCount);
        }

        private int entrySubscriberCount() {
            return proxyReturnInt(entrySubscriberCount);
        }

        private void unregisterTopicSubscriber(final TopicSubscriber subscriber) {
            Long tid = topicSubscribersToTid.get(subscriber);
            if (tid == null) {
                LOG.warn("There is no subscription to unsubscribe");
                return;
            }

            hub.outBytesLock().lock();

            try {
                writeMetaDataForKnownTID(tid);
                hub.outWire().writeDocument(false, wireOut -> {
                    wireOut.writeEventName(unregisterTopicSubscriber).text("");
                });

                hub.writeSocket(hub.outWire());
            } finally {
                hub.outBytesLock().unlock();
            }
        }

    }

    @NotNull
    private String toMapUri(@NotNull final RequestContext context) {
        return "/" + context.name()
                + "?view=" + "map&keyType=" + context.keyType().getName() + "&valueType=" + context.valueType()
                .getName();
    }

    private String toSubscriptionUri(@NotNull final RequestContext context) {
        return "/" + context.name() + "?view=subscription";
    }

    private long tid = -1;
    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);

    public RemoteKVSSubscription(RequestContext context, Asset asset) {
        hub = asset.findView(TcpChannelHub.class);
        mapSubscriptionHandler = new MapSubscriptionHandler(context, hub);
        subscriptionHandler = new SubscriptionHandler(context, hub);
    }

    @Override
    public boolean needsPrevious() {
        return true;
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> store) {

    }

    @Override
    public void notifyEvent(MapEvent<K, V> mpe) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int keySubscriberCount() {
        return subscriptionHandler.keySubscriberCount();
    }

    @Override
    public int entrySubscriberCount() {
        return subscriptionHandler.entrySubscriberCount();
    }

    @Override
    public int topicSubscriberCount() {
        return subscriptionHandler.topicSubscriberCount();
    }

    @Override
    public void registerTopicSubscriber(final RequestContext rc, final TopicSubscriber<K, V> subscriber) {
        subscriptionHandler.registerTopicSubscriber(rc, subscriber);
    }

    @Override
    public void unregisterTopicSubscriber(TopicSubscriber subscriber) {
        subscriptionHandler.unregisterTopicSubscriber(subscriber);
    }

    @Override
    public void registerDownstream(EventConsumer<K, V> subscription) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(final RequestContext rc, final Subscriber<MapEvent<K, V>> subscriber) {
        mapSubscriptionHandler.registerSubscriber(rc, subscriber);
    }

    @Override
    public void unregisterSubscriber(final Subscriber<MapEvent<K, V>> subscriber) {
        mapSubscriptionHandler.unregisterSubscriber(subscriber);
    }

    @Override
    public void close() {
        hub.close();
    }
}

