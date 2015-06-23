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
import net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
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

public class RemoteKVSSubscription<K, MV, V> extends AbstractStatelessClient implements
        ObjectKVSSubscription<K, MV, V>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private final Map<Object, Long> subscribersToTid = new ConcurrentHashMap<>();
    private final Class<K> kClass;
    private final Class<V> vClass;

    public RemoteKVSSubscription(RequestContext context, Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
        kClass = context.keyType();
        vClass = context.valueType();
    }

    private static String toUri(@NotNull final RequestContext context) {
        return "/" + context.name() + "?view=subscription";
    }

    @Override
    public void registerTopicSubscriber(RequestContext rc, TopicSubscriber<K, V> subscriber) {
        final long startTime = System.currentTimeMillis();

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.outBytesLock().lock();
        try {
            long tid = writeMetaDataStartTime(startTime);
            subscribersToTid.put(subscriber, tid);
            hub.outWire().writeDocument(false, wireOut ->
                    wireOut.writeEventName(registerTopicSubscriber).marshallable(m -> {
                        m.write(() -> "keyType").typeLiteral(kClass);
                        m.write(() -> "valueType").typeLiteral(vClass);

                    }));
            hub.asyncReadSocket(tid, w -> w.readDocument(null, d -> {
                ValueIn valueIn = d.read(reply);
                valueIn.marshallable(m -> {
                    final K topic = m.read(() -> "topic").object(kClass);
                    final V message = m.read(() -> "message").object(vClass);
                    this.onEvent(topic, message, subscriber);
                });
            }));
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

    }

    private void onEvent(K topic, V message, TopicSubscriber<K, V> subscriber) {
        try {
            if (message == null)
                unregisterTopicSubscriber(subscriber);
            else
                subscriber.onMessage(topic, message);
        } catch (InvalidSubscriberException noLongerValid) {
            unregisterTopicSubscriber(subscriber);
        }
    }

    @Override
    public int topicSubscriberCount() {
        return proxyReturnInt(topicSubscriberCount);
    }

    @Override
    public int keySubscriberCount() {
        return proxyReturnInt(keySubscriberCount);
    }

    @Override
    public int entrySubscriberCount() {
        return proxyReturnInt(entrySubscriberCount);
    }

    @Override
    public void unregisterTopicSubscriber(final TopicSubscriber subscriber) {
        Long tid = subscribersToTid.get(subscriber);
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

    @Override
    public void registerSubscriber(RequestContext rc, Subscriber<MapEvent<K, V>> subscriber) {
        registerSubscriber0(rc, subscriber);
    }

    @Override
    public void registerKeySubscriber(RequestContext rc, Subscriber<K> subscriber) {
        registerSubscriber0(rc, subscriber);
    }

    void registerSubscriber0(RequestContext rc, Subscriber subscriber) {
        final long startTime = System.currentTimeMillis();

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.outBytesLock().lock();
        try {
            long tid = writeMetaDataStartTime(startTime);
            subscribersToTid.put(subscriber, tid);
            hub.outWire().writeDocument(false, wireOut ->
                    wireOut.writeEventName(subscribe).
                            typeLiteral(CLASS_ALIASES.nameFor(rc.elementType())));
            hub.asyncReadSocket(tid, w -> w.readDocument(null, d -> {
                final StringBuilder eventname = Wires.acquireStringBuilder();
                final ValueIn valueIn = d.readEventName(eventname);

                if (EventId.onEndOfSubscription.contentEquals(eventname))
                    subscriber.onEndOfSubscription();
                else if (CoreFields.reply.contentEquals(eventname)) {
                    final Class aClass = rc.elementType();

                    final Object object = (MapEvent.class.isAssignableFrom(aClass)) ? valueIn
                            .typedMarshallable()
                            : valueIn.object(rc.elementType());

                    this.onEvent(object, subscriber);
                }
            }));
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        assert !hub.outBytesLock().isHeldByCurrentThread();

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

    @Override
    public void unregisterKeySubscriber(Subscriber<K> subscriber) {
        unregisterSubscriber0(subscriber);
    }

    @Override
    public void unregisterSubscriber(Subscriber<MapEvent<K, V>> subscriber) {
        unregisterSubscriber0(subscriber);
    }

    void unregisterSubscriber0(Subscriber subscriber) {
        Long tid = subscribersToTid.get(subscriber);
        if (tid == null) {
            LOG.warn("There is subscription to unsubscribe");
            return;
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

    @Override
    public boolean needsPrevious() {
        return true;
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> store) {

    }

    @Override
    public void notifyEvent(MapEvent<K, V> mpe) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void registerDownstream(EventConsumer<K, V> subscription) {
        throw new UnsupportedOperationException("todo");
    }

}

