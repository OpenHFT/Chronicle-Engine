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

package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.EventConsumer;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.registerTopicSubscriber;
import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.unregisterTopicSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

public class RemoteKVSSubscription<K, MV, V> extends AbstractRemoteSubscription<MapEvent<K, V>> implements
        ObjectKVSSubscription<K, MV, V>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private final Class<K> kClass;
    private final Class<V> vClass;

    public RemoteKVSSubscription(@NotNull RequestContext context, @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
        kClass = context.keyType();
        vClass = context.valueType();
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        return "/" + context.fullName() + "?view=subscription";
    }

    @Override
    public void registerTopicSubscriber(RequestContext rc, @NotNull TopicSubscriber<K, V> subscriber) {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.subscribe(new AbstractAsyncSubscription(hub, csp, "Remove KV Subscription registerTopicSubscriber") {
            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {
                wireOut.writeEventName(registerTopicSubscriber).marshallable(m -> {
                    m.write(() -> "keyType").typeLiteral(kClass);
                    m.write(() -> "valueType").typeLiteral(vClass);
                });
            }

            @Override
            public void onConsumer(@NotNull final WireIn inWire) {
                inWire.readDocument(null, d -> {
                    ValueIn valueIn = d.read(reply);
                    valueIn.marshallable(m -> {
                        final K topic = m.read(() -> "topic").object(kClass);
                        final V message = m.read(() -> "message").object(vClass);
                        RemoteKVSSubscription.this.onEvent(topic, message, subscriber);
                    });
                });
            }


        });


    }

    private void onEvent(K topic, @Nullable V message, @NotNull TopicSubscriber<K, V> subscriber) {
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
    public void unregisterTopicSubscriber(final TopicSubscriber subscriber) {
        Long tid = subscribersToTid.get(subscriber);
        if (tid == null) {
            LOG.warn("There is no subscription to unsubscribe, was " + subscribersToTid.size() + " other subscriptions.");
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
    public void registerKeySubscriber(@NotNull RequestContext rc, @NotNull Subscriber<K> subscriber) {
        registerSubscriber0(rc, subscriber);
    }

    @Override
    public void unregisterKeySubscriber(Subscriber<K> subscriber) {
        unregisterSubscriber0(subscriber);
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

