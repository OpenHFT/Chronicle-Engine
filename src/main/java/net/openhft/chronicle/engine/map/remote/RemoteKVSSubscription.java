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
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

public class RemoteKVSSubscription<K, V> extends AbstractRemoteSubscription<MapEvent<K, V>>
        implements ObjectSubscription<K, V>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private final Class<K> kClass;
    private final Class<V> vClass;
    private RequestContext rc;

    public RemoteKVSSubscription(@NotNull RequestContext context, @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
        kClass = context.keyType();
        vClass = context.valueType();
        this.rc = context;
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        StringBuilder sb = Wires.acquireStringBuilder();

        String addSlash = "";
        if (context.fullName().indexOf('/') != 0) {
            addSlash = "/";
        }

        sb.append(addSlash).append(context.fullName()).append("?view=subscription");

        if (context.messageType() != String.class)
            sb.append("&messageType=").append(CLASS_ALIASES.nameFor(context.messageType()));

        if (context.elementType() != String.class)
            sb.append("&elementType=").append(CLASS_ALIASES.nameFor(context.elementType()));

        return sb.toString();

    }

    @Override
    public void registerTopicSubscriber(@NotNull RequestContext rc, @NotNull TopicSubscriber<K, V> subscriber) {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.subscribe(new AbstractAsyncSubscription(hub, csp, "Remove KV Subscription registerTopicSubscriber") {
            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {
                subscribersToTid.put(subscriber, tid());
                wireOut.writeEventName(registerTopicSubscriber).marshallable(m -> {

                    m.write(() -> "keyType").typeLiteral(kClass);
                    m.write(() -> "valueType").typeLiteral(vClass);

                    if (rc.bootstrap() != null)
                        m.writeEventName(() -> "bootstrap").bool(rc.bootstrap());

                });
            }

            @Override
            public void onConsumer(@NotNull final WireIn inWire) {

                inWire.readDocument(null, d -> {
                    StringBuilder sb = Wires.acquireStringBuilder();
                    ValueIn valueIn = d.readEventName(sb);
                    if (reply.contentEquals(sb)) {
                        valueIn.marshallable(m -> {
                            final K topic = m.read(() -> "topic").object(kClass);
                            final V message = m.read(() -> "message").object(vClass);
                            RemoteKVSSubscription.this.onEvent(topic, message, subscriber);
                        });
                    } else if (onEndOfSubscription.contentEquals(sb)) {
                        RemoteKVSSubscription.this.onEndOfSubscription();
                        hub.unsubscribe(tid());
                    }
                });
            }
        });
    }

    private void onEvent(K topic, @Nullable V message, @NotNull TopicSubscriber<K, V> subscriber) {
        try {
            subscriber.onMessage(topic, message);
        } catch (InvalidSubscriberException noLongerValid) {
            unregisterTopicSubscriber(subscriber);
        }
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull final TopicSubscriber subscriber) {
        Long tid = subscribersToTid.get(subscriber);

        if (tid == null) {
            LOG.warn("There is no subscription to unsubscribe, was " + subscribersToTid.size() + " other subscriptions.");
            return;
        }

        hub.preventSubscribeUponReconnect(tid);

        if (!hub.isOpen()) {
            hub.unsubscribe(tid);
            return;
        }

        hub.lock(() -> {
            writeMetaDataForKnownTID(tid);
            hub.outWire().writeDocument(false, wireOut -> {
                wireOut.writeEventName(unregisterTopicSubscriber).text("");
            });
        });

    }

    @Override
    public void registerKeySubscriber(@NotNull RequestContext rc, @NotNull Subscriber<K> subscriber, @NotNull Filter<K> filter) {
        registerSubscriber0(rc, subscriber, filter);
    }

    @Override
    public boolean needsPrevious() {
        return true;
    }

    @Override
    public void setKvStore(KeyValueStore<K, V> store) {

    }

    @Override
    public void notifyEvent(MapEvent<K, V> mpe) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public boolean hasSubscribers() {
        throw new UnsupportedOperationException("has subscribers, is only implemented on the " +
                "server");
    }

    @Override
    public void registerDownstream(@NotNull EventConsumer<K, V> subscription) {
        registerSubscriber(rc.clone().messageType(rc.messageType()).elementType(MapEvent.class),
                subscription::notifyEvent, Filter.empty());
    }

}

