/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.engine.server.internal.PublisherHandler;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.engine.server.internal.SubscriptionHandler.SubscriptionEventID.*;

/**
 * Created by rob on 27/06/2015.
 */
abstract class AbstractRemoteSubscription<E> extends AbstractStatelessClient implements SubscriptionCollection<E> {

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    final Map<Object, Long> subscribersToTid = new ConcurrentHashMap<>();

    /**
     * @param hub for this connection
     * @param cid used by proxies such as the entry-set
     * @param csp the uri of the request
     */
    AbstractRemoteSubscription(@NotNull TcpChannelHub hub, long cid, @NotNull String csp) {
        super(hub, cid, csp);
    }

    public void registerSubscriber(@NotNull RequestContext rc,
                                   @NotNull Subscriber<E> subscriber,
                                   @NotNull Filter<E> filter) {
        registerSubscriber0(rc, subscriber, filter);
    }

    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        unregisterSubscriber0(subscriber);
    }

    void registerSubscriber0(@NotNull RequestContext rc,
                             @NotNull Subscriber subscriber,
                             @NotNull Filter filter) {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        final Boolean bootstrap = rc.bootstrap();
        final Boolean endSubscriptionAfterBootstrap = rc.endSubscriptionAfterBootstrap();
        String csp = this.csp;

        if (bootstrap != null)
            csp = csp + "&bootstrap=" + bootstrap;
        if (endSubscriptionAfterBootstrap != null)
            csp = csp + "&endSubscriptionAfterBootstrap=" + endSubscriptionAfterBootstrap;
        if (rc.throttlePeriodMs() > 0)
            csp = csp + "&throttlePeriodMs=" + rc.throttlePeriodMs();
        if (rc.dontPersist())
            csp = csp + "&dontPersist=" + rc.dontPersist();

        hub.subscribe(new AbstractAsyncSubscription(hub, csp, this.getClass().getSimpleName()) {
            {
                subscribersToTid.put(subscriber, tid());
            }

            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {
                wireOut.writeEventName(registerSubscriber).
                        typeLiteral(CLASS_ALIASES.nameFor(rc.elementType()));
                if (!filter.isEmpty())
                    wireOut.writeEventName(() -> "filter").object(filter);
            }

            @Override
            public void onConsumer(@NotNull final WireIn inWire) {
                inWire.readDocument(null, d -> {
                    final StringBuilder eventName = Wires.acquireStringBuilder();
                    final ValueIn valueIn = d.readEventName(eventName);

                    if (PublisherHandler.EventId.onEndOfSubscription.contentEquals(eventName)) {
                        subscriber.onEndOfSubscription();
                        subscribersToTid.remove(this);
                        hub.unsubscribe(tid());

                    } else if (CoreFields.reply.contentEquals(eventName)) {
                        final Class aClass = rc.elementType();

                        final Object object = (MapEvent.class.isAssignableFrom(aClass) ||
                                (TopologicalEvent.class.isAssignableFrom(aClass))) ?
                                valueIn.typedMarshallable()
                                : valueIn.object(rc.elementType());

                        AbstractRemoteSubscription.this.onEvent(object, subscriber);
                    }
                });
            }
        });
    }

    void onEvent(@Nullable Object message, @NotNull Subscriber subscriber) {
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

    void unregisterSubscriber0(@NotNull Subscriber subscriber) {

        final Long tid = subscribersToTid.get(subscriber);

        if (tid == null) {
            Jvm.warn().on(getClass(), "There is no subscription to unsubscribe");
            return;
        }

        hub.preventSubscribeUponReconnect(tid);

        if (!hub.isOpen()) {
            hub.unsubscribe(tid);
            return;
        }

        boolean success = hub.lock(() -> {
            writeMetaDataForKnownTID(tid);
            hub.outWire().writeDocument(false, wireOut ->
                    wireOut.writeEventName(unregisterSubscriber).text(""));
        });

        if (!success)
            hub.unsubscribe(tid);

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
}

