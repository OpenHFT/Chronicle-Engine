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

import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.registerTopicSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public final class ObjectKVSubscriptionHandler extends SubscriptionHandler<SubscriptionCollection> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectKVSubscriptionHandler.class);
    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, inputTid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (registerTopicSubscriber.contentEquals(eventName)) {
            if (tidToListener.containsKey(tid)) {
                LOG.info("Duplicate topic registration for tid " + tid);
                return;
            }

            final TopicSubscriber listener = new TopicSubscriber() {
                volatile boolean subscriptionEnded;

                @Override
                public void onMessage(final Object topic, final Object message) {
                    synchronized (publisher) {
                        publisher.put(topic, publish -> {
                            publish.writeDocument(true, wire -> wire.writeEventName(tid).int64(inputTid));
                            publish.writeNotCompleteDocument(false, wire -> wire.writeEventName(reply)
                                    .marshallable(m -> {
                                        m.write(() -> "topic").object(topic);
                                        m.write(() -> "message").object(message);
                                    }));
                        });
                    }
                }

                public void onEndOfSubscription() {
                    subscriptionEnded = true;
                    synchronized (publisher) {
                        if (!publisher.isClosed()) {
                            publisher.put(null, publish -> {
                                publish.writeDocument(true, wire ->
                                        wire.writeEventName(tid).int64(inputTid));
                                publish.writeDocument(false, wire ->
                                        wire.writeEventName(EventId.onEndOfSubscription).text(""));
                            });
                        }
                    }
                }
            };

            valueIn.marshallable(m -> {
                final Class kClass = m.read(() -> "keyType").typeLiteral();
                final Class vClass = m.read(() -> "valueType").typeLiteral();

                final StringBuilder eventName = Wires.acquireStringBuilder();

                final ValueIn bootstrap = m.readEventName(eventName);
                tidToListener.put(inputTid, listener);

                if ("bootstrap".contentEquals(eventName))
                    asset.registerTopicSubscriber(requestContext.fullName()
                            + "?bootstrap=" + bootstrap.bool(), kClass, vClass, listener);
                else
                    asset.registerTopicSubscriber(requestContext.fullName(), kClass,
                            vClass, listener);
            });
            return;
        }

        if (EventId.unregisterTopicSubscriber.contentEquals(eventName)) {
            TopicSubscriber listener = (TopicSubscriber) tidToListener.remove(inputTid);
            if (listener == null) {
                LOG.warn("No subscriber to present to unsubscribe (" + inputTid + ")");
                return;
            }
            asset.unregisterTopicSubscriber(requestContext, listener);

            return;
        }

        if (before(inputTid, valueIn)) return;

        outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

        writeData(inWire.bytes(), out -> {

            if (after(eventName)) return;

            if (EventId.notifyEvent.contentEquals(eventName)) {
                ((ObjectSubscription) subscription).notifyEvent(valueIn.typedMarshallable());
                outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
            }

        });

    };

    @Override
    protected void unregisterAll() {

        tidToListener.forEach((k, listener) -> {
            if (listener instanceof TopicSubscriber)
                asset.unregisterTopicSubscriber(requestContext,
                        (TopicSubscriber) listener);
            else
                asset.unregisterSubscriber(requestContext, (Subscriber) listener);
        });
        tidToListener.clear();
    }

    void process(@NotNull final WireIn inWire,
                 @NotNull final RequestContext requestContext,
                 @NotNull final WireOutPublisher publisher,
                 @NotNull final Asset rootAsset, final long tid,
                 @NotNull final Wire outWire,
                 @NotNull final SubscriptionCollection subscription) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.subscription = subscription;
        this.requestContext = requestContext;
        this.publisher = publisher(publisher);
        this.asset = rootAsset;
        dataConsumer.accept(inWire, tid);

    }

    public enum EventId implements ParameterizeWireKey {
        registerTopicSubscriber,
        unregisterTopicSubscriber,
        onEndOfSubscription,
        notifyEvent;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }
}
