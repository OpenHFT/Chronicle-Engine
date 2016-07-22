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

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.engine.server.internal.SubscriptionHandler.SubscriptionEventID.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.WireOutPublisher.newThrottledWireOutPublisher;

/**
 * Created by rob on 28/06/2015.
 */
public class SubscriptionHandler<T extends SubscriptionCollection> extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionHandler.class);

    final StringBuilder eventName = new StringBuilder();
    final Map<Long, Object> tidToListener = new ConcurrentHashMap<>();

    Wire outWire;
    T subscription;
    WireOutPublisher publisher;
    Asset asset;

    /**
     * after writing the tid to the wire
     *
     * @param eventName the name of the event
     * @return true if processed
     */
    boolean after(@NotNull StringBuilder eventName) {

        if (topicSubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.topicSubscriberCount());
            return true;
        }

        if (keySubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.keySubscriberCount());
            return true;
        }

        if (entrySubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
            return true;
        }

        return false;
    }

    /**
     * before writing the tid to the wire
     *
     * @param tid     the tid
     * @param valueIn the value in from the wire
     * @return true if processed
     */
    boolean before(Long tid, @NotNull ValueIn valueIn) throws AssetNotFoundException {
        if (registerSubscriber.contentEquals(eventName)) {
            final Class subscriptionType = valueIn.typeLiteral();

            final StringBuilder sb = Wires.acquireStringBuilder();
            final ValueIn valueIn1 = valueIn.wireIn().readEventName(sb);

            final Filter filter = "filter".contentEquals(sb) ?
                    valueIn1.object(Filter.class) :
                    Filter.empty();

            if (tidToListener.containsKey(tid)) {
                LOG.info("Duplicate registration for tid " + tid);
                return true;
            }

            final WireOutPublisher pub =
                    (requestContext.throttlePeriodMs() == 0) ?
                            publisher :
                            newThrottledWireOutPublisher(requestContext.throttlePeriodMs(), publisher);

            Subscriber<Object> listener = new LocalSubscriber(tid, pub);
            tidToListener.put(tid, listener);
            RequestContext rc = requestContext.clone().elementType(subscriptionType);
            final SubscriptionCollection subscription = asset.acquireSubscription(rc);
            subscription.registerSubscriber(rc, listener, filter);
            return true;
        }
        if (unregisterSubscriber.contentEquals(eventName)) {
            Subscriber<Object> listener = (Subscriber) tidToListener.remove(tid);
            if (listener == null) {
                Jvm.debug().on(getClass(), "No subscriber to present to unregisterSubscriber (" + tid + ")");
                return true;
            }

            asset.unregisterSubscriber(requestContext, listener);
            return true;
        }
        return false;
    }

    @Override
    protected void unregisterAll() {
        tidToListener.forEach((k, listener) -> asset.unregisterSubscriber(requestContext,
                (Subscriber<Object>) listener));
        tidToListener.clear();
    }

    public enum SubscriptionEventID implements ParameterizeWireKey {

        registerSubscriber,
        unregisterSubscriber,
        keySubscriberCount,
        entrySubscriberCount,
        topicSubscriberCount;

        private final WireKey[] params;

        <P extends WireKey> SubscriptionEventID(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

    class LocalSubscriber implements Subscriber<Object> {
        private final Long tid;
        private final WireOutPublisher publisher;
        volatile boolean subscriptionEnded;

        LocalSubscriber(Long tid, WireOutPublisher publisher) {
            this.tid = tid;
            this.publisher = publisher;
        }

        @Override
        public void onMessage(Object e) throws InvalidSubscriberException {
            if (subscriptionEnded)
                return;

            final WriteMarshallable event = p -> {
                p.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                p.writeNotCompleteDocument(false, wire -> wire.write(reply).object(e));
            };

            final Object key = (e instanceof MapEvent) ? ((MapEvent) e).getKey() : e;
            synchronized (publisher) {
                publisher.put(key, event);
            }
        }

        @Override
        public void onEndOfSubscription() {
            subscriptionEnded = true;
            synchronized (publisher) {
                if (!publisher.isClosed()) {
                    // no more data.
                    WriteMarshallable toPublish = publish -> {
                        publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                        publish.writeDocument(false, wire ->
                                wire.writeEventName(ObjectKVSubscriptionHandler.EventId.onEndOfSubscription).text(""));
                    };

                    publisher.put(null, toPublish);
                }
            }
        }

        @NotNull
        @Override
        public String toString() {
            return "LocalSubscriber{" +
                    "tid=" + tid + '}';
        }
    }
}
